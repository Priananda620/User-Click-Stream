from flask import Blueprint
from flask_restful import Api

from resources import StatusResource

status_blueprint = Blueprint("status_bp", __name__, url_prefix="")
prediction_api = Api(status_blueprint)
prediction_api.add_resource(StatusResource, "/status")