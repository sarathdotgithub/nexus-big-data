# ManOxCo (Man Oxygen Company) - Caso de Estudio

## Visión de la compañía

ManOxCo tiene como objetivo ser la empresa líder en España en la producción y distribución de oxígeno medicinal, proporcionando un servicio confiable y respetuoso con el medio ambiente, así como con clientes, empleados, proveedores y accionistas.

## Antecedentes

En ManOxCo ocurrió un incidente de agotamiento de oxígeno líquido (LOX) en un hospital atendido por la compañía, lo que provocó la trágica muerte de un paciente.

La causa raíz del problema aún no se conoce. Sin embargo, se cree que pudo deberse a una parada de mantenimiento no autorizada en la planta de producción de LOX en Madrid, lo que interrumpió la distribución y provocó un rápido vaciado de los tanques de almacenamiento. Aun así, el hecho sigue bajo investigación.

Como consecuencia, el hospital afectado no pudo recibir el oxígeno líquido a tiempo.

## Objetivo

Para prevenir futuros incidentes de agotamiento de oxígeno, ManOxCo ha decidido contratar a una empresa que, mediante el uso de tecnología y una plataforma de Big Data, apoye el análisis de datos y la planificación predictiva para gestionar de manera eficaz los próximos 18 meses de producción, almacenamiento, distribución y consumo de LOX.

Este nuevo proceso de la empresa se denominará LCS: Liquid Control System (Sistema de Control de Líquidos).

## Datos disponibles

ManOxCo proporciona conjuntos de datos reales de ejemplo y metadatos con frecuencias predefinidas para su análisis.

1. El archivo `plants_production_export.csv` contiene información sobre la producción, almacenamiento y despacho de oxígeno líquido (LOX) en las distintas plantas de ManOxCo.
2. El archivo `lox_truck.csv` contiene información detallada sobre la flota de camiones utilizada por ManOxCo para el transporte y la distribución de oxígeno líquido (LOX) desde las plantas de producción hacia los hospitales u otros clientes.
3. El archivo `lox_delivery_export.csv` contiene el registro detallado de las entregas de oxígeno líquido (LOX) realizadas por ManOxCo a los distintos hospitales atendidos por la compañía.
4. El archivo `lox_hosp_data.csv` contiene información de referencia sobre los hospitales atendidos por ManOxCo, centrada en la capacidad de almacenamiento, las necesidades de reabastecimiento y las condiciones comerciales asociadas al suministro de oxígeno líquido (LOX).
5. El archivo `lox_hosp_cons_export.csv` registra el consumo diario de oxígeno líquido (LOX) por parte de los hospitales atendidos por ManOxCo, permitiendo analizar la demanda y el comportamiento de uso del producto en función del tiempo y la localización.
6. El archivo `plants_data.csv` contiene información detallada sobre las plantas de producción de oxígeno líquido (LOX) de ManOxCo, incluyendo su capacidad operativa, eficiencia y estructura de costos.

## Planteamiento del problema

### La implementación real implicará:

- Datos de producción y almacenamiento recopilados en tiempo real mediante una caja IoT conectada a los sistemas PLC de las plantas.
- Datos de consumo hospitalario transmitidos en vivo con una frecuencia mínima de una lectura por minuto.

### El proveedor contratado deberá proponer una solución capaz de:

- Gestionar la producción, el almacenamiento, la distribución y el consumo de LOX en múltiples ubicaciones.
- Planificar los próximos 18 meses para garantizar la eficiencia operativa.
- Predecir y prevenir de forma proactiva futuros incidentes de agotamiento de LOX, permitiendo la ejecución inmediata de acciones correctivas.
- Sugerir el mejor momento para realizar las paradas de mantenimiento de cada planta.

### Retos técnicos

- Restricciones de producción: cada planta tiene un límite máximo mensual de producción y almacenamiento.
- Paradas de mantenimiento sugeridas entre cada 2 y 3 años: la logística de entrega de la zona en mantenimiento será cubierta por las demás plantas.

| Planta | Fecha de última parada | Días en mantenimiento |
| --- | --- | --- |
| Madrid LOX Plant | 01/01/2025 | 22 días |
| Barcelona LOX Plant | 01/08/2023 | 20 días |
| Zaragoza LOX Plant | 01/06/2023 | 15 días |
| Alicante LOX Plant | 01/12/2023 | 12 días |
| Gijón LOX Plant | 01/10/2023 | 8 días |

- Fluctuaciones de la demanda: los hospitales presentan patrones variables de consumo de oxígeno y LOX.
- Integración de datos en tiempo real: procesamiento y análisis de flujos de datos de alta frecuencia.
- Logística y distribución: garantizar una planificación eficiente de la cadena de suministro para mantener una disponibilidad estable de LOX.

## Enfoque tecnológico

Cada proveedor presentará una propuesta de solución utilizando su mejor tecnología para resolver el caso planteado.

### Entregables esperados

1. Una propuesta de solución detallada que abarque la extracción, carga y procesamiento de datos, el análisis predictivo y la planificación logística.
2. Un plan integral a 18 meses para optimizar las operaciones.
3. Un mecanismo predictivo para anticipar y mitigar posibles incidentes de agotamiento (dry-out).
4. Una presentación final que demuestre los hallazgos, las conclusiones y la implementación técnica.
5. La inversión necesaria para implementar la solución propuesta y los costos operativos asociados a su funcionamiento.
6. Una hoja de ruta de alto nivel para la implementación de la solución propuesta.

## Addendum

Se deja constancia de que usted, como proveedor, ya ha sido seleccionado para ejecutar este proyecto. En consecuencia, deberá trabajar sobre el backlog, los lineamientos base y el alcance funcional definidos en el archivo `README.md` de este repositorio, tomando dicho documento como referencia principal para la ejecución de la iniciativa.

## Criterios de evaluación

Las soluciones serán evaluadas en función de:

- Viabilidad técnica e innovación.
- Inversión dentro del presupuesto.
- Claridad y efectividad de la presentación final.

ManOxCo espera propuestas de vanguardia que aprovechen estrategias basadas en datos para mejorar la eficiencia operativa y la seguridad de los pacientes.
