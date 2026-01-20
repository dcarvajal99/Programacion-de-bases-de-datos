/* ==========================================================
   Sumativa01: BLOQUE PL/SQL - GENERACIÓN DE USUARIOS Y CLAVES
   OBJETIVO: Cumplir con la Actividad Sumativa S2 (CORREGIDO)
   EJECUTAR EN: Conexión SUMATIVA_2206_P1
   ========================================================== */

-- ============================================
-- PREREQUISITOS DE EJECUCIÓN
-- ============================================
-- 1. Ejecutar primero el script de poblamiento: Script_prueba1_C.sql
-- 2. Conectarse con el siguiente comando:
--    CONN SUMATIVA_2206_P1/oracle@localhost:1521/XE
-- ============================================

-- Declaración de Variable Bind para la fecha de proceso (Requerimiento IL3)
VARIABLE b_fecha_proceso VARCHAR2(20);

-- Inicializamos la variable Bind con la fecha actual del sistema
EXEC :b_fecha_proceso := TO_CHAR(SYSDATE, 'DD/MM/YYYY'); 

/* ============================================
   DOCUMENTACIÓN DE SENTENCIAS (Requisito 6)
   ============================================
   SENTENCIAS SQL:
   1. EXECUTE IMMEDIATE 'TRUNCATE TABLE USUARIO_CLAVE'
      - Limpia tabla destino usando SQL dinámico (DDL)
      - Más eficiente que DELETE, no genera UNDO

   2. SELECT COUNT(*) INTO v_total_empleados FROM EMPLEADO
      - Obtiene total de registros para validación
      - Permite verificar integridad de la transacción

   SENTENCIAS PL/SQL:
   1. FOR r_emp IN c_empleados LOOP ... END LOOP
      - Cursor FOR LOOP implícito para iterar empleados
      - Abre, recorre y cierra el cursor automáticamente

   2. CASE v_estado_civ_id WHEN 10 THEN ... END CASE
      - Estructura condicional para determinar letras apellido
      - Implementa regla de negocio según estado civil
   ============================================ */

DECLARE
    -- Definición de variables escalares usando %TYPE (Requerimiento IL3 - Mínimo 3)
    v_run_emp       EMPLEADO.NUMRUN_EMP%TYPE;
    v_pnombre       EMPLEADO.PNOMBRE_EMP%TYPE;
    v_snombre       EMPLEADO.SNOMBRE_EMP%TYPE; -- Variable para el segundo nombre
    v_appaterno     EMPLEADO.APPATERNO_EMP%TYPE;
    v_apmaterno     EMPLEADO.APMATERNO_EMP%TYPE;
    v_sueldo        EMPLEADO.SUELDO_BASE%TYPE;
    v_fecha_cont    EMPLEADO.FECHA_CONTRATO%TYPE;
    v_fecha_nac     EMPLEADO.FECHA_NAC%TYPE;
    v_estado_civ_id EMPLEADO.ID_ESTADO_CIVIL%TYPE;
    v_dv_emp        EMPLEADO.DVRUN_EMP%TYPE;
    
    -- Variables auxiliares para la lógica de negocio
    v_nombre_est_civil ESTADO_CIVIL.NOMBRE_ESTADO_CIVIL%TYPE;
    v_usuario_gen      USUARIO_CLAVE.NOMBRE_USUARIO%TYPE;
    v_clave_gen        USUARIO_CLAVE.CLAVE_USUARIO%TYPE;
    
    -- Variables para cálculos intermedios (Evitar lógica en SQL)
    v_anios_servicio   NUMBER(3);
    v_letra_est_civil  VARCHAR2(1);
    v_letras_apellido  VARCHAR2(2);
    v_fecha_proc_date  DATE;
    v_anio_nac_calc    NUMBER(4);
    
    -- Variables para el cálculo especial de los 3 últimos dígitos del sueldo
    v_sueldo_3ultimos  NUMBER(3); 
    v_sueldo_clave_str VARCHAR2(3); 
    
    -- Variables para control de iteraciones (Requerimiento de la Guía)
    v_total_empleados      NUMBER(4) := 0;
    v_contador_iteraciones NUMBER(4) := 0;
    
    -- Cursor explícito para recorrer empleados (Requerimiento IL2)
    -- Se incluye SNOMBRE_EMP en el cursor
    CURSOR c_empleados IS
        SELECT e.ID_EMP, e.NUMRUN_EMP, e.DVRUN_EMP, e.PNOMBRE_EMP, e.SNOMBRE_EMP,
               e.APPATERNO_EMP, e.APMATERNO_EMP, e.FECHA_CONTRATO, 
               e.SUELDO_BASE, e.FECHA_NAC, e.ID_ESTADO_CIVIL,
               ec.NOMBRE_ESTADO_CIVIL
        FROM EMPLEADO e
        JOIN ESTADO_CIVIL ec ON e.ID_ESTADO_CIVIL = ec.ID_ESTADO_CIVIL
        ORDER BY e.ID_EMP ASC;

BEGIN
    -- 1. Truncar la tabla al inicio usando SQL Dinámico (Requerimiento 5 de la Pauta)
    EXECUTE IMMEDIATE 'TRUNCATE TABLE USUARIO_CLAVE';
    
    -- Obtener el total de empleados para validar al final
    SELECT COUNT(*) INTO v_total_empleados FROM EMPLEADO;
    
    -- Convertimos la variable bind a tipo DATE para usarla en cálculos
    v_fecha_proc_date := TO_DATE(:b_fecha_proceso, 'DD/MM/YYYY');

    -- Establecer punto de recuperación antes del procesamiento
    SAVEPOINT sp_inicio_proceso;

    -- Inicio del bucle para procesar empleados (Requerimiento IL2)
    FOR r_emp IN c_empleados LOOP
        
        -- Asignación a variables %TYPE
        v_run_emp       := r_emp.NUMRUN_EMP;
        v_pnombre       := r_emp.PNOMBRE_EMP;
        v_snombre       := r_emp.SNOMBRE_EMP; -- Asignamos segundo nombre
        v_appaterno     := r_emp.APPATERNO_EMP;
        v_apmaterno     := r_emp.APMATERNO_EMP;
        v_sueldo        := r_emp.SUELDO_BASE;
        v_fecha_cont    := r_emp.FECHA_CONTRATO;
        v_fecha_nac     := r_emp.FECHA_NAC;
        v_estado_civ_id := r_emp.ID_ESTADO_CIVIL;
        v_nombre_est_civil := r_emp.NOMBRE_ESTADO_CIVIL;
        v_dv_emp        := r_emp.DVRUN_EMP;

        -- =============================================
        -- LÓGICA CONSTRUCCIÓN NOMBRE DE USUARIO
        -- =============================================
        
        -- A. Primera letra estado civil en minúscula
        v_letra_est_civil := LOWER(SUBSTR(v_nombre_est_civil, 1, 1));
        
        -- B. Años trabajando en la empresa (Cálculo PL/SQL)
        v_anios_servicio := EXTRACT(YEAR FROM v_fecha_proc_date) - EXTRACT(YEAR FROM v_fecha_cont);
        
        -- Construcción base usuario
        v_usuario_gen := v_letra_est_civil || 
                         SUBSTR(v_pnombre, 1, 3) || 
                         LENGTH(v_pnombre) || 
                         '*' || 
                         SUBSTR(TO_CHAR(v_sueldo), -1) || 
                         v_dv_emp || 
                         v_anios_servicio;
                         
        -- C. Regla Condicional: Si lleva menos de 10 años, agregar una 'X'
        IF v_anios_servicio < 10 THEN
            v_usuario_gen := v_usuario_gen || 'X';
        END IF;

        -- =============================================
        -- LÓGICA CONSTRUCCIÓN CLAVE
        -- =============================================
        
        -- A. Dos letras apellido paterno según estado civil (CASE)
        CASE v_estado_civ_id
            WHEN 10 THEN -- Casado: Dos primeras
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, 1, 2));
            WHEN 60 THEN -- AUC: Dos primeras
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, 1, 2));
            WHEN 20 THEN -- Divorciado: Primera y última
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, 1, 1) || SUBSTR(v_appaterno, -1));
            WHEN 30 THEN -- Soltero: Primera y última
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, 1, 1) || SUBSTR(v_appaterno, -1));
            WHEN 40 THEN -- Viudo: Antepenúltima y penúltima
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, -3, 2));
            WHEN 50 THEN -- Separado: Dos últimas
                v_letras_apellido := LOWER(SUBSTR(v_appaterno, -2));
            ELSE
                v_letras_apellido := 'xx'; -- Fallback
        END CASE;
        
        -- B. Cálculos numéricos para la clave
        v_anio_nac_calc := EXTRACT(YEAR FROM v_fecha_nac) + 2;
        
        -- Lógica corregida para los 3 últimos dígitos del sueldo - 1
        -- 1. Extraer los últimos 3 números
        v_sueldo_3ultimos := TO_NUMBER(SUBSTR(TO_CHAR(v_sueldo), -3));
        
        -- 2. Restar 1
        v_sueldo_3ultimos := v_sueldo_3ultimos - 1;
        
        -- 3. Manejar el caso de borde (si era 000, queda -1, lo cambiamos a 999)
        IF v_sueldo_3ultimos < 0 THEN
            v_sueldo_3ultimos := 999;
        END IF;
        
        -- 4. Formatear como texto con ceros a la izquierda (ej: 041)
        v_sueldo_clave_str := TO_CHAR(v_sueldo_3ultimos, 'FM000');
        
        -- Construcción Clave
        v_clave_gen := SUBSTR(TO_CHAR(v_run_emp), 3, 1) ||
                       TO_CHAR(v_anio_nac_calc) ||
                       v_sueldo_clave_str || -- Usamos la variable formateada
                       v_letras_apellido ||
                       r_emp.ID_EMP ||
                       TO_CHAR(v_fecha_proc_date, 'MMYYYY');

        -- =============================================
        -- INSERCIÓN DE DATOS
        -- =============================================
        
        -- Documentación SQL: Inserción en tabla USUARIO_CLAVE
        -- Se concatena el segundo nombre si existe, usando TRIM para evitar espacios dobles
        INSERT INTO USUARIO_CLAVE (
            ID_EMP, NUMRUN_EMP, DVRUN_EMP, NOMBRE_EMPLEADO, NOMBRE_USUARIO, CLAVE_USUARIO
        ) VALUES (
            r_emp.ID_EMP,
            v_run_emp,
            v_dv_emp,
            TRIM(REGEXP_REPLACE(v_pnombre || ' ' || v_snombre || ' ' || v_appaterno || ' ' || v_apmaterno, '\s+', ' ')),
            v_usuario_gen,
            v_clave_gen
        );
        
        -- Mostrar resultado en pantalla (Consola DBMS)
        DBMS_OUTPUT.PUT_LINE('Procesado ID: ' || r_emp.ID_EMP || ' | Usuario: ' || v_usuario_gen);
        
        -- Incrementar contador de iteraciones
        v_contador_iteraciones := v_contador_iteraciones + 1;
        
    END LOOP;
    
    -- Confirmación de la transacción (Requerimiento: solo si coinciden los totales)
    IF v_contador_iteraciones = v_total_empleados THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('--------------------------------------------------');
        DBMS_OUTPUT.PUT_LINE('EXITO: Se procesaron ' || v_contador_iteraciones || ' empleados.');
        DBMS_OUTPUT.PUT_LINE('Transacción confirmada.');
    ELSE
        ROLLBACK TO sp_inicio_proceso;
        DBMS_OUTPUT.PUT_LINE('ERROR: Discrepancia en registros procesados. Se hizo ROLLBACK TO SAVEPOINT.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK TO sp_inicio_proceso;
        DBMS_OUTPUT.PUT_LINE('Error fatal en el proceso: ' || SQLERRM);
END;
/

-- Consulta de validación final
SELECT * FROM USUARIO_CLAVE ORDER BY ID_EMP ASC;