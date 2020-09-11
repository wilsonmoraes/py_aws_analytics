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
    def get_sql_migracao(conta_identificador_conta, tipo_migracao,
                         evento_instancia_id, evento_status: list, modulo_chave,
                         data_inicial: str, data_final: str, limit):
        sql = """
            SELECT   modulo.chave AS modulo_chave,
                     evento_instancia_id,
                     conta.identificador,
                     attributes.tipo_migracao,
                     evento_status,
                     attributes.bean_name,
                     evento_audit_at,
                     attributes.observacao
                FROM cerbero_log_ready cld
                """
        where = []
        params = {}
        if conta_identificador_conta:
            where.append("cld.conta.identificador = :conta_identificador_conta")
            params['conta_identificador_conta'] = conta_identificador_conta
        if tipo_migracao:
            where.append("cld.attributes.tipo_migracao =:tipo_migracao")
            params['tipo_migracao'] = tipo_migracao
        if evento_instancia_id:
            where.append("evento_instancia_id=:evento_instancia_id")
            params['evento_instancia_id'] = evento_instancia_id
        if evento_status:
            where.append("evento_status in :evento_status")
            params['evento_status'] = evento_status
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
        params['limit'] = 10 if limit is None else limit

        if where:
            sql = "{} WHERE {} ORDER BY  cld.evento_audit_at desc  ".format(sql, ' AND '.join(where))
        sql = sql + " LIMIT :limit"
        return sql, params

    def listar_migracoes(self, conta_identificador_conta, tipo_migracao,
                         evento_instancia_id, evento_status, modulo_chave,
                         data_inicial, data_final, limit):
        if not data_inicial or not data_final:
            data_inicial = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
            data_final = (datetime.now() + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")

        response = []
        sql, params = self.get_sql_migracao(conta_identificador_conta=conta_identificador_conta,
                                            tipo_migracao=tipo_migracao,
                                            evento_instancia_id=evento_instancia_id,
                                            evento_status=evento_status,
                                            modulo_chave=modulo_chave,
                                            data_inicial=data_inicial,
                                            data_final=data_final,
                                            limit=limit)
        with self.db_session() as session:
            result = session.execute(sql, params)
            Record = collections.namedtuple('Record', result.keys())
            for row in result.fetchall():
                record = Record(*row)
                response.append({
                    'evento_instancia_id': record.evento_instancia_id,
                    'modulo_chave': record.modulo_chave,
                    'identificador_conta': record.identificador,
                    'tipo_migracao': record.tipo_migracao,
                    'evento_status': record.evento_status,
                    'bean_name': record.bean_name,
                    'evento_audit_at': datetime.strptime(record.evento_audit_at, '%Y-%m-%d %H:%M:%S').strftime(
                        '%d/%m/%Y %H:%M:%S') if record.evento_audit_at else '',
                    'observacao': record.observacao})
        return response
