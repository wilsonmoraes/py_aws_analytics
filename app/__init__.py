from flask import Flask

from app.mod_hello.views import home_bp
from app.mod_migracao.services import CerberoLogDataService
from app.mod_migracao.views import migracao_bp
from app.mod_pedido.views import pedido_bp


def create_app():
    base_url = '/artnew-analytics/v1'
    boilerplateapp = Flask(__name__)
    boilerplateapp.config['JSON_AS_ASCII'] = False
    boilerplateapp.config['SQLALCHEMY_ECHO'] = True

    boilerplateapp.register_blueprint(home_bp, url_prefix='{}/'.format(base_url))
    boilerplateapp.register_blueprint(migracao_bp, url_prefix='{}/migracao'.format(base_url))
    boilerplateapp.register_blueprint(pedido_bp, url_prefix='{}/pedido'.format(base_url))
    return boilerplateapp

# @create_app.route('/')
# def hello_world():
#
