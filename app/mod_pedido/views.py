from urllib.parse import unquote_plus

from flask import Blueprint
from flask_restful import reqparse

from app.helpers import helpers
from app.helpers.helpers import parse_params
from app.mod_pedido.services import CerberoLogDataService

pedido_bp = Blueprint('pedido_bp', __name__)


@pedido_bp.route('/', methods=['GET'])
@parse_params(reqparse.Argument('limit', default=100, type=int),
              reqparse.Argument('conta_identificador_conta', type=str),
              reqparse.Argument('usuario_email', type=str),
              reqparse.Argument('evento_status', type=str),
              reqparse.Argument('modulo_chave', type=str),
              reqparse.Argument('data_inicial', type=str),
              reqparse.Argument('data_final', type=str, ))
def get_all_com_erros(url_params):
    service = CerberoLogDataService()

    if url_params.evento_status:
        url_params.evento_status = unquote_plus(url_params.evento_status).replace('[', '').replace(']', '')
        url_params.evento_status = [s.strip() for s in
                                    url_params.evento_status.split(',')] if url_params.evento_status else None

    migracoes = service.listar_pedidos(conta_identificador_conta=url_params.conta_identificador_conta,
                                       evento_status=url_params.evento_status,
                                       modulo_chave=url_params.modulo_chave,
                                       usuario_email=url_params.usuario_email,
                                       data_inicial=unquote_plus(
                                           url_params.data_inicial) if url_params.data_inicial else None,
                                       data_final=unquote_plus(
                                           url_params.data_final) if url_params.data_inicial else None,
                                       limit=url_params.limit)
    return helpers.to_json_utf8(migracoes)
