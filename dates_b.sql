{{ 
    config(
        alias = 'fechas',
        materialized = 'table',
        sort = 'fecha'
    )
}}


with

cat_fechas as (
    select *
    from {{ ref('cat_fechas') }}
)

select
    extract('epoch' from (fecha)) as fecha_id,
    fecha,
    habil_mx as es_dia_habil_mx,
    habil_usa as es_dia_habil_usa,
    datepart('year', fecha) as periodo_anual,
    datepart('quarter', fecha) as periodo_cuarto,
    'Q' + to_char(fecha, 'Q') as nombre_cuarto,
    'Q' + to_char(fecha, 'Q') + ' ' + to_char(fecha, 'YYYY') as nombre_cuarto_anual,
    datepart('month', fecha) as mes,
    to_char(fecha, 'Month') as mes_nombre,
    to_char(fecha, 'Mon') as mes_corto,
    datediff('day', date_trunc('month', fecha), last_day(fecha)) + 1 as dias_en_mes,
    case
        when date_trunc('month', fecha) < date_trunc('month', current_date)
            then 0
        when date_trunc('month', fecha) < date_trunc('month', current_date)
            then datediff('day', date_trunc('month', fecha), last_day(fecha)) +1
        else datediff('day', current_date, last_day(fecha)) + 1
    end as dias_restantes_en_mes,
    cast(date_trunc('month', fecha) as date) as inicio_mes_periodo,
    last_day(fecha) as fin_mes_periodo,
    datepart('week', date_add('day', 1, fecha)) as semana,
    datepart('day', fecha) as dia,
    datepart('dayofyear', fecha) as dia_anual,
    datepart('dayofweek', fecha) + 1 as dia_semana,
    to_char(fecha, 'Day') as dia_texto,
    to_char(fecha, 'Dy') as dia_texto_abreviado,
    case
        when datepart('dayofweek', fecha) + 1 = 1 or datepart('dayofweek', fecha) + 1 = 7
            then 'Fin de semana'
        else 'Entre Semana'
        end as entre_o_fin_semana,
    decode(extract(month from fecha),
        1, 'Enero',
        2, 'Febrero',
        3, 'Marzo',
        4, 'Abril',
        5, 'Mayo',
        6, 'Junio',
        7, 'Julio',
        8, 'Agosto',
        9, 'Septiembre',
        10, 'Octubre',
        11, 'Noviembre',
        12, 'Diciembre') as mes_operacion
from cat_fechas
