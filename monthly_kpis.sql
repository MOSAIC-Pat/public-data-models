{{ 
    config(
        alias = 'indicadores_generales',
        materialized = 'table',
        schema = 'public'
    )
}}

with 

empleados_mes as (
    select
        inicio_mes_periodo,
        date_part(year, inicio_mes_periodo) as periodo_anual,
        'Tramitaciones' as empresa,
        count(distinct empleado_id) - sum(cuenta_alta) as headcount_inicio,
        sum(cuenta_baja) as headcount_bajas,
        sum(cuenta_alta) as headcount_altas,
        count(distinct empleado_id) - headcount_bajas as headcount_fin,
        (1.0 * sum(cuenta_baja)) / ((headcount_inicio + headcount_fin) / 2.0) as rotacion_mes
    from {{ ref('empleados_mes_t') }}
    group by 1, 2
),

nomina as (
    select
        date_trunc('month', fecha_operacion) as mes_periodo,
        sum(case when categoria = 'Nomina' then detalle_percepciones else 0 end) as monto_nomina,
        sum(case when categoria = 'Vacaciones' then detalle_percepciones else 0 end) as monto_vacaciones
    from {{ ref('nomina') }}
    group by 1
),

estado_resultados as (
    select
        date_trunc('month', fecha_movimiento) as mes_periodo,
        sum(case when categoria = 'Ingresos' then monto_resultado else 0 end) as ingresos,
        sum(case when categoria = 'EBITDA' then monto_resultado else 0 end) as ebitda
    from {{ ref('estado_resultados') }}
    group by 1
),

saldos as (
    select
        mes_periodo,
        sum(saldo_acumulado) as saldo_cxc,
        sum(pedimentos_totales) as pedimentos
    from {{ ref('indicadores_clientes_mes') }}
    group by 1
),

encuestas as (
    select *
    from {{ ref('encuestas_resumen_t') }}
),

balanza_comercial as (
    select *
    from {{ ref('balanza_comercial_b') }}
)

select
    empleados_mes.inicio_mes_periodo,
    empleados_mes.periodo_anual,
    empleados_mes.empresa,
    empleados_mes.headcount_inicio,
    empleados_mes.headcount_bajas,
    empleados_mes.headcount_altas,
    empleados_mes.headcount_fin,
    empleados_mes.rotacion_mes,
    nomina.monto_nomina,
    nomina.monto_vacaciones,
    estado_resultados.ingresos,
    estado_resultados.ebitda,
    saldos.saldo_cxc,
    saldos.pedimentos,
    encuestas.total_respuestas,
    encuestas.nps_promotores,
    encuestas.nps_detractores,
    encuestas.nps_neutros,
    encuestas.nps,
    encuestas.servicio_en_general,
    encuestas.clientes_totalmente_satisfechos,
    encuestas.clientes_satisfechos,
    encuestas.encuestados,
    encuestas.encuestas_participacion,
    balanza_comercial.balanza_import_usd,
    balanza_comercial.balanza_export_usd
from empleados_mes
left join nomina
    on empleados_mes.inicio_mes_periodo::date = nomina.mes_periodo::date
left join estado_resultados
    on empleados_mes.inicio_mes_periodo = estado_resultados.mes_periodo
left join saldos
    on empleados_mes.inicio_mes_periodo = saldos.mes_periodo
left join encuestas
    on encuestas.periodo = empleados_mes.periodo_anual
left join balanza_comercial
    on empleados_mes.inicio_mes_periodo = balanza_comercial.inicio_mes_periodo
