from flask import g, request
from flask_restx import Namespace, Resource, fields, inputs, marshal
from oracle_db import Session
from oracle_db.schema.Artifact import (
    Artifact,
    ArtifactAnswer,
    ArtifactOrder,
    ArtifactProgrammingResource,
    ArtifactTestCase,
    UserArtifact,
    UserHistoryV2,
)
from oracle_db.schema.CodingFeedbackResponse import CodingFeedbackResponse
from sqlalchemy.exc import IntegrityError
from oracle.api.util import (
    do_paging,
    filter_cached_based_on_search_params,
    filter_cursor,
    get_prefetched_artifact,
    is_valid_uuid,
)
from oracle.core import ArtifactType
from oracle.core.cache import RedisClusterJson
from oracle.core.coding_feedback import CodingFeedbackManager
from oracle.core.db import get_artifact_by_guid, get_artifacts
from oracle.core.request_parsers import (
    add_artifact_search_params,
    add_pagination_params,
)
from oracle.core.util import (
    set_artifact_reference_depths,
    set_artifact_reference_fields,
)
ns = Namespace(
    "endpoints",
    " V3 APIs for retrieving oracle data.",
)
redis_cluster = RedisClusterJson()
class NestedArtifactField(fields.Raw):
    """
    A class for fields that are a guid but may optionally be artifact objects if the client requests a specific depth
    """
    field_name = ""
    join_table_class = None
    def format(self, value):
        if value and hasattr(value[0], "depth") and value[0].depth > 0:
            formatted = []
            for v in value:
                artifact = get_prefetched_artifact(v, self.field_name)
                [artifact] = set_artifact_reference_depths([artifact], v.depth - 1)
                formatted.append(marshal(artifact, ns.models["artifact"]))
            return formatted
        else:
            return [get_prefetched_artifact(v, self.field_name).guid for v in value]
    def schema(self):
        schema = super(NestedArtifactField, self).schema()
        schema["type"] = "array"
        schema["items"] = fields.String().schema()
        return schema
class SortedNestedArtifactField(NestedArtifactField):
    """
    A class for fields that are a guid but may optionally be artifact objects if the client requests a specific depth
    """
    field_name = ""
    join_table_class = None
    def format(self, value):
        if value and hasattr(value[0], "depth") and value[0].depth > 0:
            formatted = []
            for v in sorted(value, key=lambda x: x.order):
                artifact = get_prefetched_artifact(v, self.field_name)
                [artifact] = set_artifact_reference_depths([artifact], v.depth - 1)
                formatted.append(marshal(artifact, ns.models["artifact"]))
            return formatted
        else:
            return [
                get_prefetched_artifact(v, self.field_name).guid
                for v in sorted(value, key=lambda x: x.order)
            ]
class ArtifactOrderField(SortedNestedArtifactField):
    join_table_class = ArtifactOrder
    field_name = "child_id"
class ArtifactGuid(fields.Raw):
    __schema_type__ = "string"
    def format(self, value):
        if value:
            return value.guid
        return None
class ArtifactAnswerField(NestedArtifactField):
    field_name = "answer_id"
    join_table_class = ArtifactAnswer
class ArtifactTestCaseField(NestedArtifactField):
    field_name = "test_case_id"
    join_table_class = ArtifactTestCase
class ArtifactNavigational(fields.Raw):
    __schema_type__ = "object"
    def format(self, value):
        choices = [
            {
                "title": v.choice,
                "order": v.order,
                "artifact_id": v.navigation.guid if v.navigation_id else None,
            }
            for v in value
        ]
        return choices
class ArtifactProgrammingResourceField(NestedArtifactField):
    field_name = "resource_id"
    join_table_class = ArtifactProgrammingResource
artifact_model = ns.model(
    "artifact",
    {
        "id": fields.String(readonly=True, attribute="guid"),
        "type": fields.String,
        "artifacts": ArtifactOrderField(required=False, default=[]),
        "answers": ArtifactAnswerField(required=False, default=[]),
        "content": fields.String,
        "test_cases": ArtifactTestCaseField(required=False, default=[]),
        "choices": ArtifactNavigational(required=False, default=[]),
        "in_line": fields.Boolean(required=False, default=False),
        "boilerplate_code": ArtifactGuid(
            required=False, attribute="boilerplate_code_artifact", default=None
        ),
        "programming_resources": ArtifactProgrammingResourceField(
            required=False, default=[]
        ),
        "topics": fields.List(
            fields.String(attribute="topic"), required=False, default=[]
        ),
        "creation_time": fields.DateTime(readonly=True),
        "creator": fields.String,
        "title": fields.String,
        "deprecated": fields.Boolean(readonly=True),
    },
)
artifact_doc_params = {
    "type": f"One of {[_type.value for _type in ArtifactType]}",
    "content": "The actual text, etc",
    "artifacts": {
        "description": "A list of artifact ids that will be shown as part of this artifact",
        "type": "array",
    },
    "answers": {
        "description": "A list of artifact ids that will be used on correct answers",
        "type": "array",
    },
    "in_line": {
        "description": "A flag that will combine a student's submission with the exercise's boilerplate code",
        "type": "bool",
    },
    "test_cases": {
        "description": "The test cases to run (list of artifact ids), if this artifact is a programming exercise",
        "type": "array",
    },
    "choices": {
        "description": "The choices will be shown to the user, if this artifact is a navigational. Should be an array of dictionary: [{\"title\": button, \"order\": number, \"artifact_id\": guid}]",
        "type": "array",
    },
    "programming_resources": {
        "description": "Additional resource artifacts needed for executing code.",
        "type": "array",
    },
}
user_history_model = ns.model(
    "user_history",
    {
        "user": fields.String,
        "coding_feedback_response_id": fields.String,
        "artifact_id": fields.String,
        "user_artifact_id": fields.String,
        "session": fields.String,
    },
)
user_history_update_model = ns.model(
    "user_history",
    {
        "id": fields.Integer,
        "coding_feedback_response_id": fields.String,
    },
)
get_artifact_parser = ns.parser()
get_artifact_parser.add_argument(
    "depth",
    type=int,
    default=0,
    help="How many child artifacts deep to go.",
    location="values",
)
get_artifact_parser.add_argument(
    "show_deprecated",
    type=inputs.boolean,
    default=False,
    help="Whether to show deprecated artifacts or not.",
    location="values",
)
get_artifact_parser = add_artifact_search_params(get_artifact_parser)
get_artifact_parser = add_pagination_params(get_artifact_parser)
artifact_parser = ns.parser()
artifact_parser.add_argument(
    "type",
    type=str,
    choices=[artifact_type.value for artifact_type in ArtifactType],
    required=True,
)
artifact_parser.add_argument("artifacts", type=list, default=None, location="json")
artifact_parser.add_argument("answers", type=list, default=None, location="json")
artifact_parser.add_argument("content", type=str, required=True)
artifact_parser.add_argument("in_line", type=bool, required=False, default=False)
artifact_parser.add_argument("test_cases", type=list, default=None, location="json")
artifact_parser.add_argument("choices", type=list, default=None, location="json")
artifact_parser.add_argument("boilerplate_code", type=str, location="json")
artifact_parser.add_argument("programming_resources", type=list, default=None, location="json")
artifact_parser.add_argument("topics", type=list, default=None, location="json")
artifact_parser.add_argument("creator", type=str, default=None, location="json")
artifact_parser.add_argument("title", type=str, default=None, location="json")
@ns.route("/artifacts")
class Artifacts(Resource):
    @ns.expect(get_artifact_parser)
    def get(self):
        args = get_artifact_parser.parse_args()
        depth = args.pop("depth")
        show_deprecated = args.pop("show_deprecated")
        limit, offset = args.pop("limit"), args.pop("offset")
        has_more = False
        if offset < 0:
            offset = 0
        artifacts = redis_cluster.get_all_artifacts(depth)
        if artifacts:
            artifacts = filter_cached_based_on_search_params(artifacts, args)
            if not show_deprecated:
                artifacts = [artifact for artifact in artifacts if not artifact["deprecated"]]
            if limit:
                artifacts, has_more = do_paging(artifacts, offset, limit)
            return {"data": artifacts, "has_more": has_more}
        with Session(expire_on_commit=False) as session:
            all_artifacts = get_artifacts(session)
            artifacts = filter_cursor(all_artifacts, args)
            total_filtered = artifacts.count()
            if limit:
                artifacts = artifacts.offset(offset * limit).limit(limit)
                has_more = artifacts.count() < total_filtered
            artifacts = artifacts.all()
            all_artifacts = all_artifacts.all()
            # need to ensure a strong reference exists to the cached artifacts otherwise we will send out unnecessary
            # SELECT statements
            g.all_artifacts = {a.id: a for a in all_artifacts}
            if not show_deprecated:
                artifacts = [a for a in artifacts if a.deprecated == False]
            artifacts = set_artifact_reference_depths(artifacts, depth)
            artifacts = marshal(artifacts, artifact_model)
            all_artifacts = set_artifact_reference_depths(all_artifacts, depth)
            redis_cluster.set_all_artifacts(depth, marshal(all_artifacts, artifact_model))
        return {"data": artifacts, "has_more": has_more}, 200
    @ns.expect(artifact_model)
    @ns.doc(params=artifact_doc_params)
    def post(self):
        data = artifact_parser.parse_args()
        artifact = Artifact(
            content=data["content"],
            type=data["type"],
            title=data["title"],
            creator=data["creator"],
            in_line=data["in_line"],
        )
        with Session(expire_on_commit=False) as session:
            session.add(artifact)
            try:
                set_artifact_reference_fields(session, data, artifact)
            except KeyError as e:
                session.rollback()
                session.close()
                return {"message": f"Artifact with guid {e} not found, no artifact with that guid exists."}, 400
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                session.close()
                return {
                    "message": "Unable to create the artifact, there is a foreign key specified that doesn't exist."
                }, 400
            marshalled = marshal(artifact, artifact_model)
            redis_cluster.clear_cache(redis_cluster.ALL_ARTIFACTS_PREFIX)
        return artifact.guid, 200
@ns.route("/user-history")
class UserHistories(Resource):
    @ns.expect(user_history_update_model)
    def put(self):
        data = request.get_json()
        with Session(expire_on_commit=False) as session:
            coding_feedback_response_manager = CodingFeedbackManager(session)
            history_id = data.get("id", None)
            coding_feedback_response_guid = data.get("coding_feedback_response_id", None)
            coding_feedback_response = coding_feedback_response_manager.get_coding_feedback_response_by_guid(coding_feedback_response_guid)
            if coding_feedback_response:
                session.query(UserHistoryV2).filter(UserHistoryV2.id == history_id).update({"coding_feedback_response_id": coding_feedback_response.id}, synchronize_session="fetch")
                session.commit()
            else:
                return {"message": "CodingFeedbackResponse not found"}, 400
        return {"message": "User history successfully updated"}, 200
