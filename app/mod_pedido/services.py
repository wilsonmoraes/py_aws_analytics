import collections
from datetime import datetime
from datetime import timedelta

from pypika import Table

from app.common.database import BaseService
from app.common.enums import Tables


class CerberoLogDataService(BaseService):
    def get_table(self):
        return Table(Tables.CERBERO_LOG_DATA.value)

    @staticmethod
    def get_sql_pedido(conta_identificador_conta,
                       usuario_email: str,
                       evento_status: list, modulo_chave,
                       data_inicial: str, data_final: str, limit):
        sql = """
            SELECT   modulo.chave AS modulo_chave,
                     conta.identificador,
                     evento_status,
                     evento_audit_at,
                     attributes.pedido.temp_id as pedido_temp_id,
                     attributes.observacao,                     
                     usuario.origem_email as usuario_email                     
                FROM cerbero_log_ready cld
                """
        where = []
        params = {}
        if conta_identificador_conta:
            where.append("cld.conta.identificador = :conta_identificador_conta")
            params['conta_identificador_conta'] = conta_identificador_conta
        if evento_status:
            where.append("evento_status in :evento_status")
            params['evento_status'] = evento_status
        if usuario_email:
            where.append("cld.usuario.origem_email like :usuario_email")
            params['usuario_email'] = '%{}%'.format(usuario_email)
        if data_inicial and data_final:
            where.append("cld.evento_audit_at BETWEEN :data_inicial AND :data_final")
            where.append("cld.year BETWEEN :year_inicial AND :year_final")
            where.append("cld.month BETWEEN :month_inicial AND :month_final")
            where.append("cld.day BETWEEN :day_inicial AND :day_final")
            params['data_inicial'] = data_inicial
            params['data_final'] = data_final
            params['year_inicial'] = data_inicial[0:4]
            params['year_final'] = data_final[0:4]
            params['month_inicial'] = data_inicial[5:7]
            params['month_final'] = data_final[5:7]
            params['day_inicial'] = data_inicial[8:10]
            params['day_final'] = data_final[8:10]
        if modulo_chave:
            where.append("cld.modulo.chave=:modulo_chave")
            params['modulo_chave'] = modulo_chave

        where.append("cld.evento_chave IN ('LOG_ENVIO_PEDIDO')")

        params['limit'] = 10 if limit is None else limit

        if where:
            sql = "{} WHERE {} ORDER BY  cld.evento_audit_at desc  ".format(sql, ' AND '.join(where))
        sql = sql + " LIMIT :limit"

        return sql, params

    def listar_pedidos(self, conta_identificador_conta,
                       evento_status, modulo_chave,
                       usuario_email,
                       data_inicial, data_final, limit):
        if not data_inicial or not data_final:
            data_inicial = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
            data_final = (datetime.now() + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")

        response = []
        sql, params = self.get_sql_pedido(conta_identificador_conta=conta_identificador_conta,
                                          evento_status=evento_status,
                                          modulo_chave=modulo_chave,
                                          data_inicial=data_inicial,
                                          data_final=data_final,
                                          usuario_email=usuario_email,
                                          limit=limit)
        with self.db_session() as session:
            result = session.execute(sql, params)
            Record = collections.namedtuple('Record', result.keys())
            for row in result.fetchall():
                record = Record(*row)
                response.append({
                    'modulo_chave': record.modulo_chave,
                    'identificador_conta': record.identificador,
                    'evento_status': record.evento_status,
                    'usuario_email': record.usuario_email,
                    'evento_audit_at': datetime.strptime(record.evento_audit_at, '%Y-%m-%d %H:%M:%S').strftime(
                        '%d/%m/%Y %H:%M:%S') if record.evento_audit_at else '',
                    'observacao': record.observacao})
        return response
