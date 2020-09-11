import json
import hashlib
import uuid
from functools import wraps

from flask_restful import reqparse
from sqlalchemy.inspection import inspect

"""
into Model: class Usuario(db.Model, Serializer):

 def get_user(id):
    user = User.query.get(id)
    return json.dumps(user.serialize())

 def get_users():
    users = User.query.all()
    return json.dumps(User.serialize_list(users))
"""


class DbResultSerializer(object):
    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]


def parse_params(*arguments):
    def parse(func):

        @wraps(func)
        def resource_verb(*args, **kwargs):
            parser = reqparse.RequestParser()
            for argument in arguments:
                if argument.required and not argument.help:
                    argument.help = 'This field cannot be blank'
                parser.add_argument(argument)
            url_params = parser.parse_args()
            return func(url_params, *args, **kwargs)

        return resource_verb

    return parse


def hash_password(submitted_password: str):
    # uuid is used to generate a random number
    salt = uuid.uuid4().hex
    return hashlib.sha256(salt.encode() + submitted_password.encode()).hexdigest() + ':' + salt


def check_password(hashed_password: str, user_password: str):
    password, salt = hashed_password.split(':')
    return password == hashlib.sha256(salt.encode() + user_password.encode()).hexdigest()


def to_json_utf8(obj):
    return json.dumps(obj, ensure_ascii=False).encode('utf8')
