# Column Reference

This document lists the columns present in the imported Excel reports and how they are mapped inside the code.

| Original column (Spanish) | Internal name | Data type | Notes |
| ------------------------- | ------------- | --------- | ----- |
| Nombre de la campaña | campaign | string | mapped via `norm_map` |
| Nombre del conjunto de anuncios | adset | string | mapped via `norm_map` |
| Nombre del anuncio | ad | string | mapped via `norm_map` |
| Día | date | date | detected automatically |
| Importe gastado (EUR) | spend | numeric | mapped via `norm_map` |
| Entrega de la campaña | campaign_delivery_status | string | mapped via `norm_map`; may be used as fallback for `entrega` |
| Entrega del conjunto de anuncios | adset_delivery_status | string | mapped via `norm_map`; may be used as fallback for `entrega` |
| Entrega del anuncio | entrega | string | primary delivery status |
| Impresiones | impr | numeric | mapped via `norm_map` |
| Alcance | reach | numeric | mapped via `norm_map` |
| Frecuencia | freq | numeric | mapped via `norm_map` |
| Valor de conversión de compras | value | numeric | mapped via `norm_map`; if absent, derived from `Valor de conversión de compras promedio` × `Compras` |
| Valor de conversión de compras promedio | value_avg | numeric | mapped via `norm_map` |
| Compras | purchases | numeric | mapped via `norm_map` |
| Visitas a la página de destino | visits | numeric | mapped via `norm_map` |
| Clics (todos) | clicks | numeric | **not mapped** (currently ignored) |
| Clics salientes | clicks_out | numeric | mapped via `norm_map` |
| CPM (costo por mil impresiones) | - | numeric | not used |
| CTR (todos) | ctr_unico_todos | numeric | mapped via `norm_map` |
| CPC (todos) | - | numeric | not used |
| Reproducciones de video de 3 segundos | rv3 | numeric | mapped via `norm_map` |
| Pagos iniciados | checkout | numeric | mapped via `norm_map` |
| ROAS (retorno de la inversión en publicidad) de compras | roas | numeric | mapped via `norm_map` |
| Porcentaje de compras por visitas a la página de destino | purchase_rate | numeric | derived metric |
| Me gusta en Facebook | - | numeric | not used |
| Visitas al perfil de Instagram | - | numeric | not used |
| Artículos agregados al carrito | addcart | numeric | mapped via `norm_map` |
| Pagos iniciados en el sitio web | checkout | numeric | mapped via `norm_map` |
| Presupuesto de la campaña | - | string | not used |
| Tipo de presupuesto de la campaña | - | string | not used |
| Públicos personalizados incluidos | aud_in -> Público In | string | mapped via `norm_map`, normalized to `Públicos In` |
| Públicos personalizados excluidos | aud_ex -> Público Ex | string | mapped via `norm_map`, normalized to `Públicos Ex` |
| Reproducciones de video hasta el 25% | rv25 | numeric | mapped via `norm_map` |
| Reproducciones de video hasta el 75% | rv75 | numeric | mapped via `norm_map` |
| Reproducciones de video hasta el 100% | rv100 | numeric | mapped via `norm_map` |
| Tiempo promedio de reproducción del video | rtime | numeric | mapped via `norm_map` |
| Clics en el enlace | clicks | numeric | mapped via `norm_map` |
| Información de pago agregada | checkout | numeric | mapped via `norm_map` (as part of checkout metrics) |
| Interacción con la página | - | numeric | not used |
| Comentarios de publicaciones / Comentarios | comentarios | numeric | mapped via `norm_map` |
| Interacciones con la publicación / Interacciones | interacciones | numeric | mapped via `norm_map` |
| Reacciones a publicaciones | - | numeric | not used |
| Veces que se guardaron las publicaciones | - | numeric | not used |
| Veces que se compartieron las publicaciones | - | numeric | not used |
| ThruPlays | thruplays | numeric | mapped via `norm_map` |
| CTR único (todos) | ctr_unico_todos | numeric | mapped via `norm_map` |
| Puja | puja | numeric | mapped via `norm_map` (also accepts "Bid") |
| Tipo de puja | - | string | not used |
| URL del sitio web / URL | url_final | string | mapped via `norm_map` |
| CTR (porcentaje de clics en el enlace) | - | numeric | not used |
| Divisa | - | string | not used; symbol extracted from `Importe gastado` |
| Interes | interest | numeric | mapped via `norm_map` |
| Deseo | deseo | numeric | mapped via `norm_map` |
| Atencion | attention | numeric | mapped via `norm_map` |
| Inicio del informe | - | date | not used |
| Fin del informe | - | date | not used |
| Estado de la entrega | delivery_general_status | string | mapped via `norm_map` |
| presupuesto Campaña | campaign_budget | numeric | mapped via `norm_map` |
| Presupuesto Adset | adset_budget | numeric | mapped via `norm_map` |
| Objetivo | objective | string | mapped via `norm_map` |
| Tipo de compra | purchase_type | string | mapped via `norm_map` |

Only the columns explicitly mapped in `norm_map` are processed. The rest are currently ignored by the data loaders.
