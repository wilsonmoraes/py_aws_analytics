from urllib.parse import unquote_plus

from flask import Blueprint
from flask_restful import reqparse

from app.mod_migracao.services import CerberoLogDataService
from app.helpers import helpers
from app.helpers.helpers import parse_params

migracao_bp = Blueprint('migracao_bp', __name__)


@migracao_bp.route('/', methods=['GET'])
@parse_params(reqparse.Argument('limit', default=100, type=int),
              reqparse.Argument('conta_identificador_conta', type=str),
              reqparse.Argument('tipo_migracao', type=str),
              reqparse.Argument('evento_instancia_id', type=str),
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

    migracoes = service.listar_migracoes(conta_identificador_conta=url_params.conta_identificador_conta,
                                         tipo_migracao=url_params.tipo_migracao,
                                         evento_instancia_id=url_params.evento_instancia_id,
                                         evento_status=url_params.evento_status,
                                         modulo_chave=url_params.modulo_chave,
                                         data_inicial=unquote_plus(
                                             url_params.data_inicial) if url_params.data_inicial else None,
                                         data_final=unquote_plus(
                                             url_params.data_final) if url_params.data_inicial else None,
                                         limit=url_params.limit)
    return helpers.to_json_utf8(migracoes)
