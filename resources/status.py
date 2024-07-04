from flask import Response, request
from flask_restful import Resource


class StatusResource(Resource):
    def get(self)->Response:
        response_data = {'message': 'Live Update Flask Kafka Listener Is Running'}
        return response_data
