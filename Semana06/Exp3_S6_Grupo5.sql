--se crea la conexión y se crea el usuario PRY2206_P6
--se ejecuta el script: crea_pobla_tablas_bd_AINTEGRAED.sql para crear y poblar las tablas del Modelo de Datos 

--PASO 1: Procedimiento para insertar en GASTO_COMUN_PAGO_CERO
--Primero, creamos un procedimiento auxiliar que se encargue de insertar un registro en la tabla GASTO_COMUN_PAGO_CERO

CREATE OR REPLACE PROCEDURE sp_inserta_gasto_comun_pago_cero (
    p_anno_mes_pcgc    IN GASTO_COMUN_PAGO_CERO.anno_mes_pcgc%TYPE,
    p_id_edif          IN GASTO_COMUN_PAGO_CERO.id_edif%TYPE,
    p_nombre_edif      IN GASTO_COMUN_PAGO_CERO.nombre_edif%TYPE,
    p_run_administrador IN GASTO_COMUN_PAGO_CERO.run_administrador%TYPE,
    p_nombre_administrador IN GASTO_COMUN_PAGO_CERO.nombre_admnistrador%TYPE,
    p_nro_depto        IN GASTO_COMUN_PAGO_CERO.nro_depto%TYPE,
    p_run_responsable  IN GASTO_COMUN_PAGO_CERO.run_responsable_pago_gc%TYPE,
    p_nombre_responsable IN GASTO_COMUN_PAGO_CERO.nombre_responsable_pago_gc%TYPE,
    p_valor_multa      IN GASTO_COMUN_PAGO_CERO.valor_multa_pago_cero%TYPE,
    p_observacion      IN GASTO_COMUN_PAGO_CERO.observacion%TYPE
) IS
    -- Este procedimiento simplemente inserta una fila en la tabla GASTO_COMUN_PAGO_CERO.
   
BEGIN
    INSERT INTO GASTO_COMUN_PAGO_CERO (
        anno_mes_pcgc,
        id_edif,
        nombre_edif,
        run_administrador,
        nombre_admnistrador,
        nro_depto,
        run_responsable_pago_gc,
        nombre_responsable_pago_gc,
        valor_multa_pago_cero,
        observacion
    ) VALUES (
        p_anno_mes_pcgc,
        p_id_edif,
        p_nombre_edif,
        p_run_administrador,
        p_nombre_administrador,
        p_nro_depto,
        p_run_responsable,
        p_nombre_responsable,
        p_valor_multa,
        p_observacion
    );
    COMMIT; -- Se confirma la transacción.
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        -- Si el registro ya existe, se podría actualizar o simplemente ignorar.
        -- Para este caso, se decide actualizar la multa y observación.
        UPDATE GASTO_COMUN_PAGO_CERO
        SET valor_multa_pago_cero = p_valor_multa,
            observacion = p_observacion
        WHERE anno_mes_pcgc = p_anno_mes_pcgc
          AND id_edif = p_id_edif
          AND nro_depto = p_nro_depto;
        COMMIT;
    WHEN OTHERS THEN
        -- Manejo de errores genérico. 
        ROLLBACK;
        RAISE; -- Relanza la excepción para que el procedimiento principal la capture.
END sp_inserta_gasto_comun_pago_cero;
/

---PASO 2: Procedimiento principal sp_procesa_deudores_gc
---Este es el procedimiento principal que recopila toda la lógica de negocio

CREATE OR REPLACE PROCEDURE sp_procesa_deudores_gc (
    p_anno_mes_periodo_cobro IN NUMBER,
    p_valor_uf_en_pesos IN NUMBER
) IS
    -- =========================================================================
    -- CONSTANTES - OBSERVACIONES (máx 80 caracteres)
    -- =========================================================================
    C_MULTA_1_MES_NO_PAGO CONSTANT NUMBER := 2;
    C_MULTA_2_MESES_NO_PAGO CONSTANT NUMBER := 4;
    C_OBS_1_MES CONSTANT VARCHAR2(80) := 'Corte combustible y agua por no pago 1 mes';
    C_OBS_2_MESES CONSTANT VARCHAR2(80) := 'Corte servicios + multa 4 UF por 2+ meses sin pago';

    -- =========================================================================
    -- VARIABLES LOCALES
    -- =========================================================================
    v_anno_periodo_anterior NUMBER(6);
    v_multa_calculada NUMBER(10);
    v_meses_sin_pago NUMBER;
    v_observacion VARCHAR2(80);
    v_periodo_anterior_existe BOOLEAN := FALSE;
    v_monto_total_gc_periodo_anterior GASTO_COMUN.monto_total_gc%TYPE;

    -- Cursor para obtener los departamentos que no pagaron en el período anterior
    CURSOR c_deudores_periodo_anterior IS
        SELECT
            gc.anno_mes_pcgc,
            gc.id_edif,
            e.nombre_edif,
            a.numrun_adm,
            a.dvrun_adm,
            a.pnombre_adm,
            a.snombre_adm,
            a.appaterno_adm,
            a.apmaterno_adm,
            gc.nro_depto,
            r.numrun_rpgc,
            r.dvrun_rpgc,
            r.pnombre_rpgc,
            r.snombre_rpgc,
            r.appaterno_rpgc,
            r.apmaterno_rpgc,
            gc.monto_total_gc
        FROM GASTO_COMUN gc
        INNER JOIN DEPARTAMENTO d ON gc.id_edif = d.id_edif AND gc.nro_depto = d.nro_depto
        INNER JOIN EDIFICIO e ON gc.id_edif = e.id_edif
        INNER JOIN ADMINISTRADOR a ON e.numrun_adm = a.numrun_adm
        INNER JOIN RESPONSABLE_PAGO_GASTO_COMUN r ON gc.numrun_rpgc = r.numrun_rpgc
        WHERE gc.anno_mes_pcgc = v_anno_periodo_anterior
          AND gc.id_epago IN (2, 3)
          AND NOT EXISTS (
              SELECT 1
              FROM PAGO_GASTO_COMUN pgc
              WHERE pgc.anno_mes_pcgc = gc.anno_mes_pcgc
                AND pgc.id_edif = gc.id_edif
                AND pgc.nro_depto = gc.nro_depto
          )
        ORDER BY e.nombre_edif ASC, gc.nro_depto ASC;

    v_run_admin_completo VARCHAR2(20);
    v_run_responsable_completo VARCHAR2(20);
    v_nombre_admin_completo VARCHAR2(200);
    v_nombre_responsable_completo VARCHAR2(200);

BEGIN
    -- =========================================================================
    -- 1. Validaciones iniciales y cálculos de fechas/períodos
    -- =========================================================================
    DBMS_OUTPUT.PUT_LINE('Iniciando proceso para el período de cobro: ' || p_anno_mes_periodo_cobro);

    v_anno_periodo_anterior := p_anno_mes_periodo_cobro - 1;
    DBMS_OUTPUT.PUT_LINE('Período base para análisis (anterior): ' || v_anno_periodo_anterior);

    BEGIN
        SELECT MAX(monto_total_gc) INTO v_monto_total_gc_periodo_anterior
        FROM GASTO_COMUN
        WHERE anno_mes_pcgc = v_anno_periodo_anterior;

        IF v_monto_total_gc_periodo_anterior IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ATENCIÓN: El período anterior (' || v_anno_periodo_anterior || ') no tiene registros en GASTO_COMUN. Se detiene el proceso.');
            RETURN;
        ELSE
            v_periodo_anterior_existe := TRUE;
            DBMS_OUTPUT.PUT_LINE('Período anterior encontrado. Monto total de referencia: ' || v_monto_total_gc_periodo_anterior);
        END IF;
    END;

    -- =========================================================================
    -- 2. Limpiar la tabla GASTO_COMUN_PAGO_CERO para el período actual
    -- =========================================================================
    DELETE FROM GASTO_COMUN_PAGO_CERO
    WHERE anno_mes_pcgc = p_anno_mes_periodo_cobro;
    DBMS_OUTPUT.PUT_LINE('Registros antiguos para el período ' || p_anno_mes_periodo_cobro || ' eliminados de GASTO_COMUN_PAGO_CERO.');
    COMMIT;

    -- =========================================================================
    -- 3. Bucle principal: Recorrer los deudores del período anterior
    -- =========================================================================
    FOR deudor IN c_deudores_periodo_anterior LOOP

        -- 3.1 Construir RUN y Nombres completos
        v_run_admin_completo := deudor.numrun_adm || '-' || deudor.dvrun_adm;
        v_run_responsable_completo := deudor.numrun_rpgc || '-' || deudor.dvrun_rpgc;

        v_nombre_admin_completo := deudor.pnombre_adm || ' ' ||
                                   NVL(deudor.snombre_adm || ' ', '') ||
                                   deudor.appaterno_adm || ' ' ||
                                   NVL(deudor.apmaterno_adm, '');
        v_nombre_responsable_completo := deudor.pnombre_rpgc || ' ' ||
                                         NVL(deudor.snombre_rpgc || ' ', '') ||
                                         deudor.appaterno_rpgc || ' ' ||
                                         NVL(deudor.apmaterno_rpgc, '');

        -- =========================================================================
        -- 4. Estructuras de Control Condicionales para determinar la multa y observación
        -- =========================================================================
        DBMS_OUTPUT.PUT_LINE('Procesando deudor: Edif ' || deudor.id_edif || ', Depto ' || deudor.nro_depto);

        SELECT COUNT(1) INTO v_meses_sin_pago
        FROM GASTO_COMUN gc
        WHERE gc.id_edif = deudor.id_edif
          AND gc.nro_depto = deudor.nro_depto
          AND gc.numrun_rpgc = deudor.numrun_rpgc
          AND gc.anno_mes_pcgc IN (v_anno_periodo_anterior, p_anno_mes_periodo_cobro)
          AND gc.id_epago IN (2, 3)
          AND NOT EXISTS (
              SELECT 1
              FROM PAGO_GASTO_COMUN pgc
              WHERE pgc.anno_mes_pcgc = gc.anno_mes_pcgc
                AND pgc.id_edif = gc.id_edif
                AND pgc.nro_depto = gc.nro_depto
          );

        DBMS_OUTPUT.PUT_LINE('Meses sin pago contabilizados: ' || v_meses_sin_pago);

        -- Aplicar reglas de negocio
        IF v_meses_sin_pago >= 2 THEN
            v_multa_calculada := C_MULTA_2_MESES_NO_PAGO * p_valor_uf_en_pesos;
            v_observacion := C_OBS_2_MESES;
            DBMS_OUTPUT.PUT_LINE('Aplica multa de 4 UF por 2 o más periodos sin pagar.');
        ELSIF v_meses_sin_pago = 1 THEN
            v_multa_calculada := C_MULTA_1_MES_NO_PAGO * p_valor_uf_en_pesos;
            v_observacion := C_OBS_1_MES;
            DBMS_OUTPUT.PUT_LINE('Aplica multa de 2 UF por 1 periodo sin pagar.');
        ELSE
            v_multa_calculada := 0;
            v_observacion := 'Sin multa';
            DBMS_OUTPUT.PUT_LINE('Caso no contemplado. Multa 0.');
        END IF;

        -- =========================================================================
        -- 5. Insertar en GASTO_COMUN_PAGO_CERO
        -- =========================================================================
        INSERT INTO GASTO_COMUN_PAGO_CERO (
            anno_mes_pcgc, id_edif, nombre_edif, run_administrador,
            nombre_admnistrador, nro_depto, run_responsable_pago_gc,
            nombre_responsable_pago_gc, valor_multa_pago_cero, observacion
        ) VALUES (
            p_anno_mes_periodo_cobro,
            deudor.id_edif,
            deudor.nombre_edif,
            v_run_admin_completo,
            v_nombre_admin_completo,
            deudor.nro_depto,
            v_run_responsable_completo,
            v_nombre_responsable_completo,
            v_multa_calculada,
            v_observacion
        );

        -- =========================================================================
        -- 6. Actualizar el valor de la multa en el GASTO_COMUN del período actual
        -- =========================================================================
        UPDATE GASTO_COMUN gc
        SET gc.multa_gc = v_multa_calculada
        WHERE gc.anno_mes_pcgc = p_anno_mes_periodo_cobro
          AND gc.id_edif = deudor.id_edif
          AND gc.nro_depto = deudor.nro_depto;

        DBMS_OUTPUT.PUT_LINE('GASTO_COMUN actualizado para el período actual.');
        COMMIT;

    END LOOP;

    -- =========================================================================
    -- 7. Finalización
    -- =========================================================================
    DBMS_OUTPUT.PUT_LINE('Proceso finalizado correctamente para el período: ' || p_anno_mes_periodo_cobro);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR CRÍTICO: ' || SQLERRM);
        RAISE;
END sp_procesa_deudores_gc;
/


--- PASO 3: verificacion de los resultados
--- vamos a ejecutar las siguientes consultas para revisar


SET SERVEROUTPUT ON;

DECLARE
    v_periodo_mayo_2026 NUMBER(6) := 202605;
    v_uf_valor CONSTANT NUMBER := 29509;
BEGIN
    sp_procesa_deudores_gc(202605, 29509);
END;
/

-- Ver los resultados
SELECT * FROM GASTO_COMUN_PAGO_CERO 
WHERE anno_mes_pcgc = 202605
ORDER BY nombre_edif, nro_depto;


-- Ver los registros insertados en GASTO_COMUN_PAGO_CERO
SELECT 
    anno_mes_pcgc,
    nombre_edif,
    nro_depto,
    run_responsable_pago_gc,
    nombre_responsable_pago_gc,
    valor_multa_pago_cero,
    observacion
FROM GASTO_COMUN_PAGO_CERO 
WHERE anno_mes_pcgc = 202605
ORDER BY nombre_edif, nro_depto;

-- Ver las multas actualizadas en GASTO_COMUN
SELECT 
    gc.anno_mes_pcgc,
    e.nombre_edif,
    gc.nro_depto,
    gc.multa_gc,
    ep.descripcion_epago
FROM GASTO_COMUN gc
JOIN EDIFICIO e ON gc.id_edif = e.id_edif
JOIN ESTADO_PAGO ep ON gc.id_epago = ep.id_epago
WHERE gc.anno_mes_pcgc = 202605
  AND gc.multa_gc > 0
ORDER BY e.nombre_edif, gc.nro_depto;

-- Resumen de multas por edificio
SELECT 
    e.nombre_edif,
    COUNT(*) as deptos_multados,
    SUM(gc.multa_gc) as total_multas
FROM GASTO_COMUN gc
JOIN EDIFICIO e ON gc.id_edif = e.id_edif
WHERE gc.anno_mes_pcgc = 202605
  AND gc.multa_gc > 0
GROUP BY e.nombre_edif
ORDER BY e.nombre_edif;

