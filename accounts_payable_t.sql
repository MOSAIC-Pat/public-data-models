{{ 
    config(
        alias = 'vial_saldos_cxc',
        materialized = 'table',
        schema = 'transform',
        sort = 'cliente_id'
    )
}}


SELECT
    left(vial_clientes.descripcion_cliente,32):: varchar(32) AS descripcion_cliente,
    coalesce(vial_clientes.grupo_cliente, vial_clientes.descripcion_cliente) as grupo_cliente,
    vial_saldos_cxc.*,
    vial_clientes.dias_de_credito,
    datediff('day', vial_saldos_cxc.fecha, current_date) AS dias_diferencia,
    datediff('day', vial_saldos_cxc.fecha + coalesce(vial_clientes.dias_de_credito,0), current_date) AS dias_diferencia_credito,
    CASE
        WHEN vial_saldos_cxc.credito_abono = '+'
            THEN vial_saldos_cxc.monto END AS cargos,
    CASE
        WHEN vial_saldos_cxc.credito_abono = '-'
            THEN vial_saldos_cxc.monto END AS abonos,
    CASE
        WHEN vial_saldos_cxc.numero_cuenta_gastos is null
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS movimientos_trafico_no_facturado,
    CASE
        WHEN vial_saldos_cxc.numero_cuenta_gastos is null AND vial_saldos_cxc.tipo_de_operacion !=1
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS movimientos_trafico_no_facturado_anticipo,
    CASE
        WHEN dias_diferencia < 0
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado  ELSE 0 END AS por_vencer,
    CASE
        WHEN dias_diferencia <= 30 AND dias_diferencia >= 0
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado ELSE 0 END AS rango_30,
    CASE
        WHEN dias_diferencia <= 60 AND dias_diferencia > 30
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado ELSE 0 END AS rango_60,
    CASE
        WHEN dias_diferencia <= 90 AND dias_diferencia > 60
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado ELSE 0 END AS rango_90,
    CASE
        WHEN dias_diferencia <= 120 AND dias_diferencia > 90
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado ELSE 0 END AS rango_120,
    CASE
        WHEN dias_diferencia > 120
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) - movimientos_trafico_no_facturado ELSE 0 END AS mayor_120,
    CASE
        WHEN dias_diferencia_credito < 0
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS por_vencer_credito,
    CASE
        WHEN dias_diferencia_credito <= 30 AND dias_diferencia_credito >= 0
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS rango_30_credito,
    CASE
        WHEN dias_diferencia_credito <= 60 AND dias_diferencia_credito > 30
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS rango_60_credito,
    CASE
        WHEN dias_diferencia_credito <= 90 AND dias_diferencia_credito > 60
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS rango_90_credito,
    CASE
        WHEN dias_diferencia_credito <= 120 AND dias_diferencia_credito > 90
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS rango_120_credito,
    CASE
        WHEN dias_diferencia_credito > 120
            THEN coalesce(cargos, 0) - coalesce(abonos, 0) ELSE 0 END AS mayor_120_credito
FROM {{ ref('vial_saldos_cxc_b') }} AS vial_saldos_cxc
LEFT JOIN {{ ref('vial_clientes_b') }} AS vial_clientes
    ON vial_clientes.cliente_id = vial_saldos_cxc.cliente_id
