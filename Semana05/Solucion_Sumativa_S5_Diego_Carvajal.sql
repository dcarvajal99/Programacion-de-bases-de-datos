-- ============================================================================
-- ACTIVIDAD SUMATIVA SEMANA 05 - PRY2206
-- Procesamiento de Avances y Súper Avances para Reporte SBIF
-- Alumno: Diego Carvajal
-- Fecha: 08/02/2026
-- ============================================================================

SET SERVEROUTPUT ON;

-- Variable BIND para periodo de ejecución (año a procesar)
VARIABLE v_periodo_ejecucion NUMBER;
EXEC :v_periodo_ejecucion := EXTRACT(YEAR FROM SYSDATE);

-- ============================================================================
-- BLOQUE PL/SQL ANÓNIMO PRINCIPAL
-- ============================================================================
DECLARE
    -- ========================================================================
    -- DECLARACIÓN DE VARIABLES
    -- ========================================================================
    
    -- Variables para información del cliente y transacción
    v_numrun NUMBER;
    v_dvrun VARCHAR2(1);
    v_nro_tarjeta NUMBER;
    v_nro_transaccion NUMBER;
    v_fecha_transaccion DATE;
    v_tipo_transaccion VARCHAR2(40);
    v_monto_transaccion NUMBER;
    v_monto_total_transaccion NUMBER;
    v_aporte_sbif NUMBER;
    v_porcentaje_aporte NUMBER;
    
    -- Variables para control de proceso
    v_contador_iteraciones NUMBER := 0;
    v_total_registros NUMBER := 0;
    v_periodo_proceso NUMBER;
    
    -- ========================================================================
    -- VARRAY para tipos de transacción (códigos)
    -- ========================================================================
    TYPE tipo_transacciones_array IS VARRAY(2) OF NUMBER(4);
    v_tipos_transaccion tipo_transacciones_array;
    
    -- ========================================================================
    -- REGISTRO PL/SQL para resumen mensual
    -- ========================================================================
    TYPE tipo_resumen_mensual IS RECORD (
        mes_anno VARCHAR2(6),
        tipo_transaccion VARCHAR2(40),
        monto_total NUMBER,
        aporte_total NUMBER
    );
    v_resumen tipo_resumen_mensual;
    
    -- ========================================================================
    -- CURSOR EXPLÍCITO 1: Obtener todas las transacciones de avances del año
    -- ========================================================================
    CURSOR c_transacciones_avances IS
        SELECT c.numrun,
               c.dvrun,
               ttc.nro_tarjeta,
               ttc.nro_transaccion,
               ttc.fecha_transaccion,
               ttt.nombre_tptran_tarjeta AS tipo_transaccion,
               ttc.monto_transaccion,
               ttc.monto_total_transaccion
        FROM TRANSACCION_TARJETA_CLIENTE ttc
        JOIN TARJETA_CLIENTE tc ON ttc.nro_tarjeta = tc.nro_tarjeta
        JOIN CLIENTE c ON tc.numrun = c.numrun
        JOIN TIPO_TRANSACCION_TARJETA ttt ON ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = :v_periodo_ejecucion
          AND ttc.cod_tptran_tarjeta IN (102, 103)
        ORDER BY ttc.fecha_transaccion, c.numrun;
    
    -- ========================================================================
    -- CURSOR EXPLÍCITO 2 CON PARÁMETRO: Obtener resumen por mes y tipo
    -- ========================================================================
    CURSOR c_resumen_mensual(p_mes_anno VARCHAR2, p_cod_tptran NUMBER) IS
        SELECT p_mes_anno AS mes_anno,
               MIN(tipo_transaccion) AS tipo_transaccion,
               SUM(monto_transaccion) AS monto_total,
               SUM(aporte_sbif) AS aporte_total
        FROM DETALLE_APORTE_SBIF
        WHERE TO_CHAR(fecha_transaccion, 'MMYYYY') = p_mes_anno
          AND tipo_transaccion IN (
              SELECT nombre_tptran_tarjeta
              FROM TIPO_TRANSACCION_TARJETA
              WHERE cod_tptran_tarjeta = p_cod_tptran
          )
        GROUP BY p_mes_anno;
    
    -- ========================================================================
    -- EXCEPCIONES
    -- ========================================================================
    -- Excepción definida por el usuario
    ex_periodo_invalido EXCEPTION;
    
    -- Excepción no predefinida
    ex_tabla_no_existe EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_tabla_no_existe, -00942);
    
    -- ========================================================================
    -- FUNCIÓN: Obtener porcentaje de aporte según tramo
    -- ========================================================================
    FUNCTION obtener_porcentaje_aporte(p_monto NUMBER) RETURN NUMBER IS
        v_porcentaje NUMBER := 0;
    BEGIN
        -- Buscar el porcentaje de aporte según el tramo del monto
        SELECT porc_aporte_sbif
        INTO v_porcentaje
        FROM TRAMO_APORTE_SBIF
        WHERE p_monto BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;
        
        RETURN v_porcentaje;
    EXCEPTION
        -- Excepción predefinida: NO_DATA_FOUND
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ADVERTENCIA: No se encontró tramo para monto $' || p_monto);
            RETURN 0;
    END obtener_porcentaje_aporte;
    
    -- ========================================================================
    -- FUNCIÓN: Calcular aporte SBIF
    -- ========================================================================
    FUNCTION calcular_aporte_sbif(p_monto_total NUMBER, p_porcentaje NUMBER) RETURN NUMBER IS
    BEGIN
        -- Calcular aporte redondeado a entero
        RETURN ROUND(p_monto_total * p_porcentaje / 100);
    END calcular_aporte_sbif;

BEGIN
    -- ========================================================================
    -- INICIALIZACIÓN
    -- ========================================================================
    
    -- Obtener periodo de ejecución desde variable BIND
    v_periodo_proceso := :v_periodo_ejecucion;
    
    -- Validar periodo
    IF v_periodo_proceso IS NULL OR v_periodo_proceso < 2020 THEN
        RAISE ex_periodo_invalido;
    END IF;
    
    -- Inicializar VARRAY con codigos: 102=Avance, 103=Super Avance.
    -- Evita depender de tildes/codificacion del cliente.
    v_tipos_transaccion := tipo_transacciones_array(102, 103);
    
    -- Inicializar registro de resumen
    v_resumen.monto_total := 0;
    v_resumen.aporte_total := 0;
    
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('PROCESAMIENTO DE APORTES SBIF');
    DBMS_OUTPUT.PUT_LINE('Periodo: ' || v_periodo_proceso);
    DBMS_OUTPUT.PUT_LINE('Fecha de ejecución: ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================================================
    -- TRUNCAR TABLAS DE DESTINO
    -- ========================================================================
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    DBMS_OUTPUT.PUT_LINE('Tablas truncadas exitosamente.');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================================================
    -- CONTAR TOTAL DE REGISTROS A PROCESAR
    -- ========================================================================
    SELECT COUNT(*)
    INTO v_total_registros
    FROM TRANSACCION_TARJETA_CLIENTE ttc,
         TIPO_TRANSACCION_TARJETA ttt
    WHERE ttc.cod_tptran_tarjeta = ttt.cod_tptran_tarjeta
      AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_periodo_proceso
      AND ttc.cod_tptran_tarjeta IN (102, 103);
    
    DBMS_OUTPUT.PUT_LINE('Total de transacciones a procesar: ' || v_total_registros);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================================================
    -- PROCESAMIENTO DE TRANSACCIONES (CURSOR 1)
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('Iniciando procesamiento de transacciones...');
    DBMS_OUTPUT.PUT_LINE('');
    
    FOR rec IN c_transacciones_avances LOOP
        -- Incrementar contador de iteraciones
        v_contador_iteraciones := v_contador_iteraciones + 1;
        
        -- Asignar valores del registro a variables
        v_numrun := rec.numrun;
        v_dvrun := rec.dvrun;
        v_nro_tarjeta := rec.nro_tarjeta;
        v_nro_transaccion := rec.nro_transaccion;
        v_fecha_transaccion := rec.fecha_transaccion;
        v_tipo_transaccion := rec.tipo_transaccion;
        v_monto_transaccion := rec.monto_transaccion;
        v_monto_total_transaccion := rec.monto_total_transaccion;
        
        -- Obtener porcentaje de aporte según tramo
        v_porcentaje_aporte := obtener_porcentaje_aporte(v_monto_total_transaccion);
        
        -- Calcular aporte SBIF (redondeado a entero)
        v_aporte_sbif := calcular_aporte_sbif(v_monto_total_transaccion, v_porcentaje_aporte);
        
        -- Insertar en tabla DETALLE_APORTE_SBIF
        INSERT INTO DETALLE_APORTE_SBIF (
            numrun,
            dvrun,
            nro_tarjeta,
            nro_transaccion,
            fecha_transaccion,
            tipo_transaccion,
            monto_transaccion,
            aporte_sbif
        ) VALUES (
            v_numrun,
            v_dvrun,
            v_nro_tarjeta,
            v_nro_transaccion,
            v_fecha_transaccion,
            v_tipo_transaccion,
            -- Se almacena el monto total de la transaccion (incluye interes),
            -- tal como lo muestra la Figura 1 de la actividad.
            v_monto_total_transaccion,
            v_aporte_sbif
        );
        
        -- Mostrar progreso cada 10 registros
        IF MOD(v_contador_iteraciones, 10) = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Procesados ' || v_contador_iteraciones || ' de ' || v_total_registros || ' registros...');
        END IF;
        
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Procesamiento de detalle completado: ' || v_contador_iteraciones || ' registros.');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================================================
    -- GENERACIÓN DE RESUMEN MENSUAL (CURSOR 2 CON PARÁMETRO)
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('Generando resumen mensual...');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Procesar resumen para cada mes del año
    FOR v_mes IN 1..12 LOOP
        DECLARE
            v_mes_anno VARCHAR2(6);
        BEGIN
            -- Formato requerido por RESUMEN_APORTE_SBIF: MMYYYY
            v_mes_anno := LPAD(v_mes, 2, '0') || v_periodo_proceso;
            
            -- Procesar cada tipo de transacción del VARRAY
            FOR i IN 1..v_tipos_transaccion.COUNT LOOP
                -- Abrir cursor con parámetros
                FOR rec_resumen IN c_resumen_mensual(v_mes_anno, v_tipos_transaccion(i)) LOOP
                    -- Uso explicito de registro PL/SQL para cumplir requerimiento.
                    v_resumen.mes_anno := rec_resumen.mes_anno;
                    v_resumen.tipo_transaccion := rec_resumen.tipo_transaccion;
                    v_resumen.monto_total := rec_resumen.monto_total;
                    v_resumen.aporte_total := rec_resumen.aporte_total;
                    
                    -- Insertar en tabla RESUMEN_APORTE_SBIF
                    INSERT INTO RESUMEN_APORTE_SBIF (
                        mes_anno,
                        tipo_transaccion,
                        monto_total_transacciones,
                        aporte_total_abif
                    ) VALUES (
                        v_resumen.mes_anno,
                        v_resumen.tipo_transaccion,
                        v_resumen.monto_total,
                        v_resumen.aporte_total
                    );
                    
                    DBMS_OUTPUT.PUT_LINE('Resumen ' || v_mes_anno || ' - ' || 
                                       rec_resumen.tipo_transaccion || ': $' || 
                                       rec_resumen.monto_total || ' (Aporte: $' || 
                                       rec_resumen.aporte_total || ')');
                END LOOP;
            END LOOP;
            
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Resumen mensual generado exitosamente.');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================================================
    -- VALIDACIÓN Y COMMIT
    -- ========================================================================
    
    -- Verificar que se procesaron todos los registros
    IF v_contador_iteraciones = v_total_registros THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('PROCESO COMPLETADO EXITOSAMENTE');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('Total de transacciones procesadas: ' || v_contador_iteraciones);
        DBMS_OUTPUT.PUT_LINE('Transacciones confirmadas (COMMIT realizado)');
        DBMS_OUTPUT.PUT_LINE('============================================================');
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: No se procesaron todos los registros.');
        DBMS_OUTPUT.PUT_LINE('Esperados: ' || v_total_registros || ', Procesados: ' || v_contador_iteraciones);
        DBMS_OUTPUT.PUT_LINE('Transacciones revertidas (ROLLBACK)');
    END IF;
    
EXCEPTION
    -- ========================================================================
    -- MANEJO DE EXCEPCIONES
    -- ========================================================================
    
    -- Excepción definida por el usuario
    WHEN ex_periodo_invalido THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: Periodo de ejecución inválido: ' || v_periodo_proceso);
        DBMS_OUTPUT.PUT_LINE('El periodo debe ser un año válido (>= 2020)');
        
    -- Excepción no predefinida
    WHEN ex_tabla_no_existe THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: Una o más tablas requeridas no existen.');
        DBMS_OUTPUT.PUT_LINE('Verifique que el script de creación se haya ejecutado correctamente.');
        
    -- Excepción predefinida (capturada en función, pero también aquí por seguridad)
    WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: No se encontraron datos para procesar.');
        DBMS_OUTPUT.PUT_LINE('Verifique que existan transacciones para el periodo ' || v_periodo_proceso);
        
    -- Cualquier otra excepción
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR INESPERADO: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Código de error: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('Transacciones revertidas (ROLLBACK)');
        RAISE;
        
END;
/

-- ============================================================================
-- VERIFICACIÓN DE RESULTADOS
-- ============================================================================

PROMPT
PROMPT ============================================================
PROMPT VERIFICACIÓN DE RESULTADOS - TABLA DETALLE_APORTE_SBIF
PROMPT ============================================================
PROMPT

SELECT numrun,
       dvrun,
       nro_tarjeta,
       nro_transaccion,
       TO_CHAR(fecha_transaccion, 'DD/MM/YYYY') AS fecha_transaccion,
       tipo_transaccion,
       monto_transaccion AS monto_total_transaccion,
       aporte_sbif
FROM DETALLE_APORTE_SBIF
ORDER BY DETALLE_APORTE_SBIF.fecha_transaccion, numrun;

PROMPT
PROMPT ============================================================
PROMPT VERIFICACIÓN DE RESULTADOS - TABLA RESUMEN_APORTE_SBIF
PROMPT ============================================================
PROMPT

SELECT mes_anno,
       tipo_transaccion,
       monto_total_transacciones,
       aporte_total_abif
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;

PROMPT
PROMPT ============================================================
PROMPT ESTADÍSTICAS GENERALES
PROMPT ============================================================
PROMPT

SELECT 'Total registros en DETALLE_APORTE_SBIF' AS descripcion,
       COUNT(*) AS cantidad
FROM DETALLE_APORTE_SBIF
UNION ALL
SELECT 'Total registros en RESUMEN_APORTE_SBIF' AS descripcion,
       COUNT(*) AS cantidad
FROM RESUMEN_APORTE_SBIF
UNION ALL
SELECT 'Monto total de aportes SBIF' AS descripcion,
       SUM(aporte_total_abif) AS cantidad
FROM RESUMEN_APORTE_SBIF;

PROMPT
PROMPT ============================================================
PROMPT FIN DEL SCRIPT
PROMPT ============================================================
