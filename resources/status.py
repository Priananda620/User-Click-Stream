from flask import Response, request
from flask_restful import Resource


class StatusResource(Resource):
    def get(self)->Response:
        response_data = {'message': 'Hello, this is the API!'}
        return response_data
