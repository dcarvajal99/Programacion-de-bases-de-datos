--Se realiza la conexion y crea el usuario PRY2206_P3--
--se crea _pobla_tablas_bd_CLINICA_KETEKURA.SQL--

-- CASO 1--
-- Bloque PL/SQL anónimo para procesar morosidad de pagos del año anterior
DECLARE
    -- VARRAY para almacenar los valores de multas por especialidad
    TYPE tipo_multas IS VARRAY(8) OF NUMBER;
    v_multas_especialidad tipo_multas;
    
    -- se realizan las variables para almacenar valores
    v_anno_param NUMBER;
    v_dias_morosidad NUMBER;
    v_valor_multa NUMBER;
    v_valor_multa_descuento NUMBER;
    v_edad_paciente NUMBER;
    v_porcentaje_descto NUMBER := 0;
    v_nombre_especialidad VARCHAR2(40);
    
    -- Registro PL/SQL para almacenar datos del cursor
    TYPE reg_morosidad IS RECORD (
        pac_run PACIENTE.pac_run%TYPE,
        dv_run PACIENTE.dv_run%TYPE,
        nombre_paciente VARCHAR2(60),
        ate_id ATENCION.ate_id%TYPE,
        fecha_venc_pago PAGO_ATENCION.fecha_venc_pago%TYPE,
        fecha_pago PAGO_ATENCION.fecha_pago%TYPE,
        especialidad_atencion ESPECIALIDAD.nombre%TYPE,
        edad_paciente NUMBER
    );
    
    v_registro reg_morosidad;
    
    -- Cursor explícito para obtener atenciones pagadas fuera de plazo
    CURSOR c_morosidad (p_anno NUMBER) IS
        SELECT p.pac_run,
               p.dv_run,
               p.pnombre || ' ' || p.snombre || ' ' || p.apaterno || ' ' || p.amaterno AS nombre_paciente,
               a.ate_id,
               pa.fecha_venc_pago,
               pa.fecha_pago,
               e.nombre AS especialidad_atencion,
               TRUNC(MONTHS_BETWEEN(pa.fecha_pago, p.fecha_nacimiento)/12) AS edad_paciente
        FROM PACIENTE p
        JOIN ATENCION a ON p.pac_run = a.pac_run
        JOIN PAGO_ATENCION pa ON a.ate_id = pa.ate_id
        JOIN ESPECIALIDAD e ON a.esp_id = e.esp_id
        WHERE pa.fecha_pago > pa.fecha_venc_pago
          AND EXTRACT(YEAR FROM pa.fecha_pago) = p_anno
        ORDER BY pa.fecha_venc_pago ASC, p.apaterno ASC;
    
    -- Función para obtener el índice de la multa según especialidad
    FUNCTION obtener_indice_multas(p_especialidad VARCHAR2) RETURN NUMBER IS
    BEGIN
        IF p_especialidad IN ('Cirugía General', 'Dermatología') THEN
            RETURN 1;
        ELSIF p_especialidad = 'Ortopedia y Traumatología' THEN
            RETURN 2;
        ELSIF p_especialidad IN ('Inmunología', 'Otorrinolaringología') THEN
            RETURN 3;
        ELSIF p_especialidad IN ('Fisiatría', 'Medicina Interna') THEN
            RETURN 4;
        ELSIF p_especialidad = 'Medicina General' THEN
            RETURN 5;
        ELSIF p_especialidad = 'Psiquiatría Adultos' THEN
            RETURN 6;
        ELSIF p_especialidad IN ('Cirugía Digestiva', 'Reumatología') THEN
            RETURN 7;
        ELSE
            RETURN 1; -- Valor por defecto
        END IF;
    END obtener_indice_multas;
    
    -- Función para calcular porcentaje de descuento por edad
    FUNCTION calcular_descuento_edad(p_edad NUMBER) RETURN NUMBER IS
        v_porcentaje NUMBER := 0;
    BEGIN
        -- Buscar en tabla de descuentos por edad
        SELECT NVL(MAX(porcentaje_descto), 0)
        INTO v_porcentaje
        FROM PORC_DESCTO_3RA_EDAD
        WHERE p_edad BETWEEN anno_ini AND anno_ter;
        
        RETURN v_porcentaje;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END calcular_descuento_edad;

BEGIN
    -- TRUNCAR la tabla PAGO_MOROSO
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PAGO_MOROSO';
    
    -- Inicializar VARRAY con valores de multas
    -- de acuerdo a la  tabla los datos son:
    -- 1. Cirugía General y Dermatología: $1.200
    -- 2. Ortopedia y Traumatología: $1.300
    -- 3. Inmunología y Otorrinolaringología: $1.700
    -- 4. Fisiatría y Medicina Interna: $1.900
    -- 5. Medicina General: $1.100
    -- 6. Psiquiatría Adultos: $2.000
    -- 7. Cirugía Digestiva y Reumatología: $2.300
    v_multas_especialidad := tipo_multas(1200, 1300, 1700, 1900, 1100, 2000, 2300);
    
    -- Obtener año paramétrico (año anterior al actual)
    v_anno_param := EXTRACT(YEAR FROM SYSDATE) - 1;
    
    -- Procesar cursor para obtener atenciones con morosidad
    OPEN c_morosidad(v_anno_param);
    
    LOOP
        FETCH c_morosidad INTO v_registro;
        EXIT WHEN c_morosidad%NOTFOUND;
        
        -- se va a calcular los días de morosidad
        v_dias_morosidad := TRUNC(v_registro.fecha_pago - v_registro.fecha_venc_pago);
        
        -- se va obtener valor de multa según especialidad
        v_valor_multa := v_multas_especialidad(obtener_indice_multas(v_registro.especialidad_atencion)) * v_dias_morosidad;
        
        -- vamos a calcular descuento por tercera edad
        v_porcentaje_descto := calcular_descuento_edad(v_registro.edad_paciente);
        
        -- se va aplicar descuento si corresponde, usamos if
        IF v_porcentaje_descto > 0 THEN
            v_valor_multa_descuento := v_valor_multa - (v_valor_multa * v_porcentaje_descto / 100);
        ELSE
            v_valor_multa_descuento := v_valor_multa;
        END IF;
        
        -- se van a insertar en tabla PAGO_MOROSO
        INSERT INTO PAGO_MOROSO (
            pac_run, 
            pac_dv_run, 
            pac_nombre, 
            ate_id, 
            fecha_venc_pago, 
            fecha_pago, 
            dias_morosidad, 
            especialidad_atencion, 
            monto_multa
        ) VALUES (
            v_registro.pac_run,
            v_registro.dv_run,
            v_registro.nombre_paciente,
            v_registro.ate_id,
            v_registro.fecha_venc_pago,
            v_registro.fecha_pago,
            v_dias_morosidad,
            v_registro.especialidad_atencion,
            v_valor_multa_descuento
        );
        
    END LOOP;
    
    CLOSE c_morosidad;
    
    -- Confirmar cambios
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Proceso completado exitosamente.');
    DBMS_OUTPUT.PUT_LINE('Año procesado: ' || v_anno_param);
    DBMS_OUTPUT.PUT_LINE('Registros insertados: ' || SQL%ROWCOUNT);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END;
/

-- Verificar los resultados
SELECT * FROM PAGO_MOROSO ORDER BY fecha_venc_pago, pac_nombre;

-- se van a revisar  filas de ejemplo
SELECT pac_nombre, especialidad_atencion, dias_morosidad, monto_multa 
FROM PAGO_MOROSO 
WHERE ROWNUM <= 10;


--CASO 2--

DECLARE
    -- Definición de tipos
    TYPE destinacion_t IS VARRAY(10) OF VARCHAR2(50);
    TYPE medico_record IS RECORD (
        unidad_nombre VARCHAR2(50),
        run_medico VARCHAR2(15),
        nombre_completo VARCHAR2(50),
        correo_institucional VARCHAR2(25),
        total_atenciones NUMBER,
        destinacion VARCHAR2(50)
    );
    
    -- Variables
    v_destinaciones destinacion_t := destinacion_t(
        'Servicio de Atención Primaria de Urgencia (SAPU)',
        'Hospitales del área de la Salud Pública',
        'Centros de Salud Familiar (CESFAM)',
        'Consultorios Generales'
    );
    
    v_max_atenciones NUMBER;
    v_anio_anterior NUMBER;
    v_medico medico_record;
    
    -- Cursor explícito para procesar todos los médicos
    CURSOR c_medicos IS
        SELECT 
            UPPER(u.nombre) AS unidad_nombre,
            m.med_run || '-' || m.dv_run AS run_medico,
            INITCAP(m.pnombre || ' ' || m.snombre || ' ' || m.apaterno || ' ' || m.amaterno) AS nombre_completo,
            m.apaterno,
            m.med_run,
            m.uni_id
        FROM medico m
        JOIN unidad u ON m.uni_id = u.uni_id
        ORDER BY u.nombre, m.apaterno;
    
    -- Función para calcular total de atenciones por médico en año anterior
    FUNCTION total_atenciones_medico(p_med_run NUMBER, p_anio NUMBER) RETURN NUMBER IS
        v_total NUMBER := 0;
    BEGIN
        SELECT COUNT(*) INTO v_total
        FROM atencion
        WHERE med_run = p_med_run
        AND EXTRACT(YEAR FROM fecha_atencion) = p_anio;
        
        RETURN v_total;
    END;
    
    -- Función para determinar destinación según unidad y atenciones
    FUNCTION determinar_destinacion(p_unidad VARCHAR2, p_atenciones NUMBER) RETURN VARCHAR2 IS
    BEGIN
        -- Reglas según Tabla 2
        IF UPPER(p_unidad) LIKE '%ADULTO%' OR UPPER(p_unidad) LIKE '%AMBULATORIA%' THEN
            RETURN v_destinaciones(1); -- SAPU
        ELSIF UPPER(p_unidad) LIKE '%URGENCIA%' THEN
            IF p_atenciones <= 3 THEN
                RETURN v_destinaciones(1); -- SAPU
            ELSE
                RETURN v_destinaciones(2); -- Hospitales
            END IF;
        ELSIF UPPER(p_unidad) LIKE '%CARDIOLOGÍA%' OR UPPER(p_unidad) LIKE '%ONCOLÓGICA%' THEN
            RETURN v_destinaciones(2); -- Hospitales
        ELSIF UPPER(p_unidad) LIKE '%CIRUGÍA%' THEN
            IF p_atenciones <= 3 THEN
                RETURN v_destinaciones(1); -- SAPU
            ELSE
                RETURN v_destinaciones(2); -- Hospitales
            END IF;
        ELSIF UPPER(p_unidad) LIKE '%CRÍTICO%' THEN
            RETURN v_destinaciones(2); -- Hospitales
        ELSIF UPPER(p_unidad) LIKE '%PSIQUIATRÍA%' OR UPPER(p_unidad) LIKE '%SALUD MENTAL%' THEN
            RETURN v_destinaciones(3); -- CESFAM
        ELSIF UPPER(p_unidad) LIKE '%TRAUMATOLOGÍA%' THEN
            IF p_atenciones <= 3 THEN
                RETURN v_destinaciones(1); -- SAPU
            ELSE
                RETURN v_destinaciones(2); -- Hospitales
            END IF;
        ELSE
            RETURN v_destinaciones(4); -- Consultorios Generales (por defecto)
        END IF;
    END;
    
    -- Función para generar correo institucional
    FUNCTION generar_correo(p_unidad VARCHAR2, p_apaterno VARCHAR2, p_med_run NUMBER) RETURN VARCHAR2 IS
        v_prefijo VARCHAR2(2);
        v_letras_apellido VARCHAR2(2);
        v_digitos_run VARCHAR2(3);
        v_correo VARCHAR2(25);
    BEGIN
        -- Dos primeras letras de la unidad (sin espacios, en mayúscula)
        v_prefijo := UPPER(SUBSTR(REPLACE(p_unidad, ' ', ''), 1, 2));
        
        -- Penúltima y antepenúltima letra del apellido paterno
        IF LENGTH(p_apaterno) >= 3 THEN
            v_letras_apellido := UPPER(
                SUBSTR(p_apaterno, LENGTH(p_apaterno)-2, 1) || 
                SUBSTR(p_apaterno, LENGTH(p_apaterno)-1, 1)
            );
        ELSE
            v_letras_apellido := UPPER(LPAD(p_apaterno, 2, 'X'));
        END IF;
        
        -- Tres últimos dígitos del RUN
        v_digitos_run := SUBSTR(TO_CHAR(p_med_run), -3, 3);
        
        -- Construir correo
        v_correo := v_prefijo || v_letras_apellido || v_digitos_run || '@medicotk.cl';
        
        -- Asegurar longitud máxima
        IF LENGTH(v_correo) > 25 THEN
            v_correo := SUBSTR(v_correo, 1, 25);
        END IF;
        
        RETURN LOWER(v_correo);
    END;

BEGIN
    -- Determinar año anterior
    v_anio_anterior := EXTRACT(YEAR FROM SYSDATE) - 1;
    
    -- Calcular máximo de atenciones en año anterior
    SELECT MAX(total_atenciones) INTO v_max_atenciones
    FROM (
        SELECT COUNT(*) AS total_atenciones
        FROM atencion
        WHERE EXTRACT(YEAR FROM fecha_atencion) = v_anio_anterior
        GROUP BY med_run
    );
    
    -- Si no hay atenciones en el año anterior, establecer máximo en 0
    IF v_max_atenciones IS NULL THEN
        v_max_atenciones := 0;
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Año procesado: ' || v_anio_anterior);
    DBMS_OUTPUT.PUT_LINE('Máximo de atenciones: ' || v_max_atenciones);
    
    -- Procesar cada médico con cursor explícito
    FOR medico_rec IN c_medicos LOOP
        -- Calcular total de atenciones del médico en año anterior
        v_medico.total_atenciones := total_atenciones_medico(medico_rec.med_run, v_anio_anterior);
        
        -- se verifica si el médico tiene menos atenciones que el máximo
        IF v_medico.total_atenciones < v_max_atenciones THEN
            -- se asignan los datos del médico
            v_medico.unidad_nombre := medico_rec.unidad_nombre;
            v_medico.run_medico := medico_rec.run_medico;
            v_medico.nombre_completo := medico_rec.nombre_completo;
            
            -- Determinar destinación
            v_medico.destinacion := determinar_destinacion(medico_rec.unidad_nombre, v_medico.total_atenciones);
            
            -- Generar correo institucional
            v_medico.correo_institucional := generar_correo(
                medico_rec.unidad_nombre, 
                medico_rec.apaterno, 
                medico_rec.med_run
            );
            
            -- Insertar en tabla destino
            INSERT INTO MEDICO_SERVICIO_COMUNIDAD (
                unidad, run_medico, nombre_medico, 
                correo_institucional, total_aten_medicas, destinacion
            ) VALUES (
                v_medico.unidad_nombre,
                v_medico.run_medico,
                v_medico.nombre_completo,
                v_medico.correo_institucional,
                v_medico.total_atenciones,
                v_medico.destinacion
            );
            
            -- mostrar información procesada
            DBMS_OUTPUT.PUT_LINE(
                'Procesado: ' || v_medico.nombre_completo || 
                ' - Atenciones: ' || v_medico.total_atenciones ||
                ' - Destino: ' || v_medico.destinacion
            );
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Proceso completado. Datos insertados en MEDICO_SERVICIO_COMUNIDAD.');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END;
/

--se verifican los resultados

SELECT * FROM MEDICO_SERVICIO_COMUNIDAD ORDER BY unidad, nombre_medico;

