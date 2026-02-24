-- se crea el ususario  PRY2206_P7
-- una vez conectado con la conexión PRY2206_P7, se ejecuta el script:  crea_pobla_tablas_bd_CLINICA_MXSALUD.sql 

--PASO 1 , Este package servirá como contenedor de la lógica de negocio para el cálculo de multas.
CREATE OR REPLACE PACKAGE gestion_moras_pkg IS
    -- Constructores públicos (Variables)
    v_valor_multa NUMBER(6); -- Almacena el valor de la multa calculado
    v_valor_descuento NUMBER(4); -- Almacena el valor del descuento aplicado (en % o valor fijo, según se defina)

    -- Función pública para obtener el porcentaje de descuento para mayores de 70 años
    FUNCTION fn_descto_3ra_edad (p_edad_paciente NUMBER) RETURN NUMBER;

    -- el requerimiento pide una Función Almacenada independiente.
END gestion_moras_pkg;
/
CREATE OR REPLACE PACKAGE BODY gestion_moras_pkg IS

    -- ***** FUNCIÓN PARA OBTENER DESCUENTO POR EDAD *****
    -- Propósito: Calcula el porcentaje de descuento para la multa basado en la edad del paciente.
    -- Utiliza la tabla PORC_DESCTO_3RA_EDAD.
    FUNCTION fn_descto_3ra_edad (p_edad_paciente NUMBER) RETURN NUMBER IS
        v_porcentaje_descuento PORC_DESCTO_3RA_EDAD.porcentaje_descto%TYPE := 0; -- Inicializa en 0 (sin descuento)
    BEGIN
        -- Estructura de control condicional para determinar el rango de edad
        IF p_edad_paciente >= 86 THEN
            SELECT porcentaje_descto INTO v_porcentaje_descuento
            FROM PORC_DESCTO_3RA_EDAD WHERE anno_ini = 86;
        ELSIF p_edad_paciente >= 76 THEN
            SELECT porcentaje_descto INTO v_porcentaje_descuento
            FROM PORC_DESCTO_3RA_EDAD WHERE anno_ini = 76;
        ELSIF p_edad_paciente >= 71 THEN
            SELECT porcentaje_descto INTO v_porcentaje_descuento
            FROM PORC_DESCTO_3RA_EDAD WHERE anno_ini = 71;
        ELSIF p_edad_paciente >= 66 THEN
            SELECT porcentaje_descto INTO v_porcentaje_descuento
            FROM PORC_DESCTO_3RA_EDAD WHERE anno_ini = 66;
        ELSIF p_edad_paciente >= 60 THEN
            SELECT porcentaje_descto INTO v_porcentaje_descuento
            FROM PORC_DESCTO_3RA_EDAD WHERE anno_ini = 60;
        ELSE
            v_porcentaje_descuento := 0; -- Menor de 60 años, sin descuento
        END IF;

        RETURN v_porcentaje_descuento;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0; -- Si no hay rango, retorna 0.
        WHEN OTHERS THEN
            RAISE; -- En un sistema real, se podría registrar en una tabla de errores.
    END fn_descto_3ra_edad;

END gestion_moras_pkg;
/

--PASO 2: Construcción de la Función Almacenada
--Esta función independiente obtiene el nombre de la especialidad.

CREATE OR REPLACE FUNCTION fn_obtener_especialidad (p_esp_id NUMBER) RETURN VARCHAR2 IS
    v_nombre_especialidad ESPECIALIDAD.nombre%TYPE;
BEGIN
    -- Estructura de control condicional implícita (SELECT INTO)
    SELECT nombre INTO v_nombre_especialidad
    FROM ESPECIALIDAD
    WHERE esp_id = p_esp_id;

    RETURN v_nombre_especialidad;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'Especialidad no encontrada';
    WHEN OTHERS THEN
        RETURN 'Error al obtener especialidad';
END fn_obtener_especialidad;
/

--PASO 3: Construcción del Procedimiento Almacenado
--Este es el núcleo de la solución. Integra todos los elementos y sigue las reglas de negocio al pie de la letra.

CREATE OR REPLACE PROCEDURE sp_generar_informe_moras IS

    -- ***** VARRAY para almacenar los valores de las multas por especialidad *****
    -- Se crea un tipo como VARRAY de números. Se asume un máximo de 20 especialidades.
    TYPE multas_varray_type IS VARRAY(20) OF NUMBER(6);
    -- Se inicializa el VARRAY con los valores de la tabla de multas en el orden de los IDs de especialidad.
    -- *** IMPORTANTE: Este orden debe coincidir con la lógica de asignación posterior. ***
    -- Usamos los IDs: 100(Trauma),200(Gastro),300(Neuro),400(Geria),500(Ofta),600(Pedia),700(Med Gral),800(Gineco),900(Derma)
    v_multas_por_especialidad multas_varray_type := multas_varray_type(1300, 2000, 1700, 1100, 1900, 1700, 1200, 2000, 2300);

    -- Variables de trabajo
    v_fecha_limite DATE; -- Fecha para filtrar el año anterior
    v_edad_paciente NUMBER(3);
    v_fecha_nac_paciente PACIENTE.fecha_nacimiento%TYPE;
    v_porc_descuento NUMBER(4);
    v_multa_calculada NUMBER(6);
    v_observacion PAGO_MOROSO.observacion%TYPE;
    v_especialidad_nombre ESPECIALIDAD.nombre%TYPE;
    v_esp_id_aux ATENCION.ate_id%TYPE; -- Variable auxiliar para obtener el ID de especialidad

    -- Cursor para recorrer las atenciones pagadas con retraso en el año anterior
    CURSOR c_atrasos_anio_anterior IS
        SELECT
            p.pac_run,
            p.dv_run,
            p.pnombre || ' ' || p.snombre || ' ' || p.apaterno || ' ' || p.amaterno AS nombre_completo,
            a.ate_id,
            pa.fecha_venc_pago,
            pa.fecha_pago,
            -- Cálculo de días de morosidad. Si fecha_pago es null, se usan los días hasta hoy.
            TRUNC(NVL(pa.fecha_pago, SYSDATE) - pa.fecha_venc_pago) AS dias_morosidad,
            a.med_run, -- Necesario para obtener la especialidad del médico
            a.costo AS costo_atencion,
            p.fecha_nacimiento -- Para calcular la edad
        FROM PAGO_ATENCION pa
        JOIN ATENCION a ON pa.ate_id = a.ate_id
        JOIN PACIENTE p ON a.pac_run = p.pac_run
        WHERE 1=1
            AND pa.fecha_pago > pa.fecha_venc_pago -- Filtra solo pagos atrasados
            AND EXTRACT(YEAR FROM pa.fecha_venc_pago) = EXTRACT(YEAR FROM SYSDATE) - 1 -- Filtra por año anterior al actual
        ORDER BY pa.fecha_venc_pago ASC, p.apaterno ASC; -- Orden requerido

BEGIN
    -- ***** LIMPIEZA INICIAL *****
    -- Truncar la tabla de destino para empezar limpio.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PAGO_MOROSO';
    DBMS_OUTPUT.PUT_LINE('Tabla PAGO_MOROSO truncada.');

    -- ***** PROCESAMIENTO DE CADA ATENCIÓN ATRASADA *****
    FOR registro IN c_atrasos_anio_anterior LOOP

        -- Inicializar variables para cada iteración
        v_multa_calculada := 0;
        v_observacion := NULL;
        gestion_moras_pkg.v_valor_descuento := 0; -- Reiniciamos la variable pública de descuento
        v_edad_paciente := TRUNC(MONTHS_BETWEEN(SYSDATE, registro.fecha_nacimiento) / 12);

        -- ***** OBTENER ESPECIALIDAD *****
        -- Necesitamos el esp_id del médico que realizó la atención
        BEGIN
            SELECT m.esp_id INTO v_esp_id_aux
            FROM MEDICO m
            WHERE m.med_run = registro.med_run;

            v_especialidad_nombre := fn_obtener_especialidad(v_esp_id_aux);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_especialidad_nombre := 'Especialidad Desconocida';
                v_esp_id_aux := 0; -- Valor para manejar el error en el VARRAY
        END;

        -- ***** CALCULAR LA MULTA BASE USANDO EL VARRAY *****
        -- Estructura de control condicional (CASE) para asignar la multa según el ID de especialidad.
        -- Los índices del VARRAY deben corresponder con los IDs.
        IF v_esp_id_aux BETWEEN 100 AND 900 THEN
            CASE v_esp_id_aux
                WHEN 100 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(1); -- Traumatología
                WHEN 200 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(2); -- Gastroenterología
                WHEN 300 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(3); -- Neurología
                WHEN 400 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(4); -- Geriatría
                WHEN 500 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(5); -- Oftalmología
                WHEN 600 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(6); -- Pediatría
                WHEN 700 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(7); -- Medicina General
                WHEN 800 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(8); -- Ginecología
                WHEN 900 THEN gestion_moras_pkg.v_valor_multa := v_multas_por_especialidad(9); -- Dermatología
                ELSE gestion_moras_pkg.v_valor_multa := 0;
            END CASE;
        ELSE
            gestion_moras_pkg.v_valor_multa := 0;
        END IF;

        -- ***** APLICAR DESCUENTO POR TERCERA EDAD *****
        IF v_edad_paciente > 70 THEN
            -- Usar la función del package para obtener el % de descuento
            gestion_moras_pkg.v_valor_descuento := gestion_moras_pkg.fn_descto_3ra_edad(v_edad_paciente);
            v_observacion := 'Paciente tenía ' || v_edad_paciente || ' años. Se aplicó descuento de ' || gestion_moras_pkg.v_valor_descuento || '% sobre la multa.';
            -- Calcular la multa final: multa base * (1 - % descuento/100)
            v_multa_calculada := gestion_moras_pkg.v_valor_multa * (1 - (gestion_moras_pkg.v_valor_descuento / 100));
            DBMS_OUTPUT.PUT_LINE('Descuento aplicado al paciente ' || registro.pac_run || '. Multa final: ' || v_multa_calculada);
        ELSE
            -- Sin descuento, la multa final es la multa base
            v_multa_calculada := gestion_moras_pkg.v_valor_multa;
            v_observacion := 'No aplica descuento por edad.';
        END IF;

        -- ***** INSERTAR EN LA TABLA PAGO_MOROSO *****
        INSERT INTO PAGO_MOROSO (
            pac_run,
            pac_dv_run,
            pac_nombre,
            ate_id,
            fecha_venc_pago,
            fecha_pago,
            dias_morosidad,
            especialidad_atencion,
            costo_atencion,
            monto_multa,
            observacion
        ) VALUES (
            registro.pac_run,
            registro.dv_run,
            registro.nombre_completo,
            registro.ate_id,
            registro.fecha_venc_pago,
            registro.fecha_pago,
            registro.dias_morosidad,
            v_especialidad_nombre,
            registro.costo_atencion,
            v_multa_calculada * registro.dias_morosidad, -- Multa total = multa diaria * días de mora
            v_observacion
        );

    END LOOP;

    -- ***** CONFIRMAR TRANSACCIÓN *****
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Proceso finalizado. Se insertaron los registros en PAGO_MOROSO.');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error en el proceso: ' || SQLERRM);
        -- Aquí se podría insertar el error en la tabla ERRORES_PROCESO
        -- INSERT INTO ERRORES_PROCESO VALUES (sec_correlativo.NEXTVAL, 'sp_generar_informe_moras', SQLERRM);
        RAISE;
END sp_generar_informe_moras;
/

--PASO 4  Triggers para integridad y auditoría
--Trigger 1: trg_pago_moroso_before_insert, Garantiza que nunca se inserten días de morosidad negativos en la tabla PAGO_MOROSO

CREATE OR REPLACE TRIGGER trg_pago_moroso_before_insert
BEFORE INSERT ON PAGO_MOROSO
FOR EACH ROW
BEGIN
    -- Asegurar que los días de morosidad no sean negativos
    IF :NEW.dias_morosidad < 0 THEN
        :NEW.dias_morosidad := 0;
        :NEW.observacion := :NEW.observacion || ' [CORREGIDO: Días de mora negativo se fijó a 0]';
    END IF;
END;
/

--PASO 5 Verificar el Package

-- Ver especificación del package
SELECT text 
FROM user_source 
WHERE name = 'GESTION_MORAS_PKG' 
AND type = 'PACKAGE'
ORDER BY line;

-- Ver cuerpo del package  
SELECT text 
FROM user_source 
WHERE name = 'GESTION_MORAS_PKG' 
AND type = 'PACKAGE BODY'
ORDER BY line;

-- Ver si el package existe y su estado
SELECT object_name, object_type, status 
FROM user_objects 
WHERE object_name = 'GESTION_MORAS_PKG';


-- Ver código de la función
SELECT text 
FROM user_source 
WHERE name = 'FN_OBTENER_ESPECIALIDAD' 
AND type = 'FUNCTION'
ORDER BY line;

--PASO 5 Verificar el procedimiento
-- Ver código del procedimiento
SELECT text 
FROM user_source 
WHERE name = 'SP_GENERAR_INFORME_MORAS' 
AND type = 'PROCEDURE'
ORDER BY line;



EXEC sp_generar_informe_moras;

SELECT COUNT(*) FROM PAGO_MOROSO;

--Ver todos los años presentes
SELECT 
    EXTRACT(YEAR FROM fecha_venc_pago) AS anio_vencimiento,
    COUNT(*) AS cantidad_atenciones
FROM PAGO_MOROSO
GROUP BY EXTRACT(YEAR FROM fecha_venc_pago)
ORDER BY anio_vencimiento;



--Ver muestra de los datos
SELECT 
    ate_id,
    TO_CHAR(fecha_venc_pago, 'DD/MM/YYYY') AS fecha_venc,
    TO_CHAR(fecha_pago, 'DD/MM/YYYY') AS fecha_pago,
    dias_morosidad,
    especialidad_atencion,
    monto_multa,
    SUBSTR(observacion, 1, 50) AS observacion_resumida
FROM PAGO_MOROSO
WHERE ROWNUM <= 5;


--pacientes con descuentos

SELECT 
    pac_nombre,
    especialidad_atencion,
    dias_morosidad,
    monto_multa,
    observacion
FROM PAGO_MOROSO
WHERE observacion LIKE '%descuento%';

--multas por especialidad

SELECT 
    especialidad_atencion,
    COUNT(*) AS cantidad,
    AVG(monto_multa) AS multa_promedio,
    MIN(monto_multa) AS multa_min,
    MAX(monto_multa) AS multa_max
FROM PAGO_MOROSO
GROUP BY especialidad_atencion
ORDER BY especialidad_atencion;