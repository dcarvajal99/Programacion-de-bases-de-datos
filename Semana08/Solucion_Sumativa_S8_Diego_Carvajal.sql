-- ============================================================================
-- SUMATIVA 03 - SEMANA 08 (PRY2206)
-- Desarrollando programas PL/SQL en base de datos
-- Entrega: trigger + package + funciones + procedimiento principal + pruebas
-- ============================================================================

SET SERVEROUTPUT ON;
SET DEFINE OFF;

-- ============================================================================
-- 0) LIMPIEZA SEGURA DE OBJETOS
-- ============================================================================
BEGIN
    EXECUTE IMMEDIATE 'DROP TRIGGER TRG_AUD_TOTAL_CONSUMOS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE <> -4080 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_COBRANZA_HOTEL';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE <> -4043 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION FN_OBT_AGENCIA_HUESPED';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE <> -4043 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION FN_OBT_CONSUMOS_USD';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE <> -4043 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PROCEDURE SP_PROCESA_COBRANZA_DIARIA';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE <> -4043 THEN
            RAISE;
        END IF;
END;
/

-- ============================================================================
-- 1) TRIGGER CASO 1
-- Mantiene TOTAL_CONSUMOS sincronizada ante INSERT/UPDATE/DELETE en CONSUMO.
-- ============================================================================
CREATE OR REPLACE TRIGGER TRG_AUD_TOTAL_CONSUMOS
AFTER INSERT OR UPDATE OR DELETE ON CONSUMO
FOR EACH ROW
DECLARE
    PROCEDURE PR_ACTUALIZA_TOTAL(
        P_ID_HUESPED IN NUMBER,
        P_DELTA      IN NUMBER
    ) IS
    BEGIN
        UPDATE TOTAL_CONSUMOS
           SET MONTO_CONSUMOS = NVL(MONTO_CONSUMOS, 0) + P_DELTA
         WHERE ID_HUESPED = P_ID_HUESPED;

        IF SQL%ROWCOUNT = 0 THEN
            INSERT INTO TOTAL_CONSUMOS (ID_HUESPED, MONTO_CONSUMOS)
            VALUES (P_ID_HUESPED, GREATEST(P_DELTA, 0));
        END IF;

        UPDATE TOTAL_CONSUMOS
           SET MONTO_CONSUMOS = GREATEST(MONTO_CONSUMOS, 0)
         WHERE ID_HUESPED = P_ID_HUESPED;
    END PR_ACTUALIZA_TOTAL;
BEGIN
    IF INSERTING THEN
        PR_ACTUALIZA_TOTAL(:NEW.ID_HUESPED, :NEW.MONTO);
    ELSIF DELETING THEN
        PR_ACTUALIZA_TOTAL(:OLD.ID_HUESPED, -:OLD.MONTO);
    ELSIF UPDATING THEN
        IF :OLD.ID_HUESPED = :NEW.ID_HUESPED THEN
            PR_ACTUALIZA_TOTAL(:NEW.ID_HUESPED, :NEW.MONTO - :OLD.MONTO);
        ELSE
            PR_ACTUALIZA_TOTAL(:OLD.ID_HUESPED, -:OLD.MONTO);
            PR_ACTUALIZA_TOTAL(:NEW.ID_HUESPED, :NEW.MONTO);
        END IF;
    END IF;
END;
/

-- ============================================================================
-- 2) PACKAGE CASO 2
-- ============================================================================
CREATE OR REPLACE PACKAGE PKG_COBRANZA_HOTEL IS
    G_MONTO_TOURS_USD NUMBER;

    FUNCTION FN_MONTO_TOURS_USD(
        P_ID_HUESPED IN NUMBER
    ) RETURN NUMBER;
END PKG_COBRANZA_HOTEL;
/

CREATE OR REPLACE PACKAGE BODY PKG_COBRANZA_HOTEL IS
    FUNCTION FN_MONTO_TOURS_USD(
        P_ID_HUESPED IN NUMBER
    ) RETURN NUMBER IS
        V_TOTAL_TOURS NUMBER := 0;
    BEGIN
        SELECT NVL(SUM(T.VALOR_TOUR * HT.NUM_PERSONAS), 0)
          INTO V_TOTAL_TOURS
          FROM HUESPED_TOUR HT
          JOIN TOUR T
            ON T.ID_TOUR = HT.ID_TOUR
         WHERE HT.ID_HUESPED = P_ID_HUESPED;

        G_MONTO_TOURS_USD := ROUND(V_TOTAL_TOURS);
        RETURN G_MONTO_TOURS_USD;
    EXCEPTION
        WHEN OTHERS THEN
            G_MONTO_TOURS_USD := 0;
            RETURN 0;
    END FN_MONTO_TOURS_USD;
END PKG_COBRANZA_HOTEL;
/

-- ============================================================================
-- 3) FUNCION AGENCIA CON LOG DE ERRORES
-- ============================================================================
CREATE OR REPLACE FUNCTION FN_OBT_AGENCIA_HUESPED(
    P_ID_HUESPED IN NUMBER
) RETURN VARCHAR2 IS
    V_AGENCIA AGENCIA.NOM_AGENCIA%TYPE;
    V_MSG_ERROR REG_ERRORES.MSG_ERROR%TYPE;
BEGIN
    SELECT A.NOM_AGENCIA
      INTO V_AGENCIA
      FROM HUESPED H
      LEFT JOIN AGENCIA A
        ON A.ID_AGENCIA = H.ID_AGENCIA
     WHERE H.ID_HUESPED = P_ID_HUESPED;

    IF V_AGENCIA IS NULL THEN
        RAISE NO_DATA_FOUND;
    END IF;

    RETURN V_AGENCIA;
EXCEPTION
    WHEN OTHERS THEN
        V_MSG_ERROR := SQLERRM;
        INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
        VALUES (
            SQ_ERROR.NEXTVAL,
            'Error en la funcion FN AGENCIA al recuperar la agencia del huesped con id ' || P_ID_HUESPED,
            V_MSG_ERROR
        );
        RETURN 'NO REGISTRA AGENCIA';
END FN_OBT_AGENCIA_HUESPED;
/

-- ============================================================================
-- 4) FUNCION CONSUMOS USD
-- ============================================================================
CREATE OR REPLACE FUNCTION FN_OBT_CONSUMOS_USD(
    P_ID_HUESPED IN NUMBER
) RETURN NUMBER IS
    V_CONSUMOS NUMBER := 0;
    V_MSG_ERROR REG_ERRORES.MSG_ERROR%TYPE;
BEGIN
    SELECT TC.MONTO_CONSUMOS
      INTO V_CONSUMOS
      FROM TOTAL_CONSUMOS TC
     WHERE TC.ID_HUESPED = P_ID_HUESPED;

    RETURN NVL(V_CONSUMOS, 0);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        V_MSG_ERROR := SQLERRM;
        INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
        VALUES (
            SQ_ERROR.NEXTVAL,
            'Error en la funcion FN CONSUMOS al recuperar los consumos del cliente con Id ' || P_ID_HUESPED,
            V_MSG_ERROR
        );
        RETURN 0;
    WHEN OTHERS THEN
        V_MSG_ERROR := SQLERRM;
        INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
        VALUES (
            SQ_ERROR.NEXTVAL,
            'Error en la funcion FN CONSUMOS al recuperar los consumos del cliente con Id ' || P_ID_HUESPED,
            V_MSG_ERROR
        );
        RETURN 0;
END FN_OBT_CONSUMOS_USD;
/

-- ============================================================================
-- 5) PROCEDIMIENTO PRINCIPAL
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_PROCESA_COBRANZA_DIARIA(
    P_FECHA_PROCESO IN DATE,
    P_VALOR_DOLAR   IN NUMBER
) IS
    V_AGENCIA              VARCHAR2(40);
    V_CONSUMOS_USD         NUMBER := 0;
    V_TOURS_USD            NUMBER := 0;
    V_CANT_PERSONAS        NUMBER := 1;
    V_COBRO_PERSONA_CLP    CONSTANT NUMBER := 35000;
    V_COBRO_PERSONA_USD    NUMBER := 0;
    V_SUBTOTAL_USD         NUMBER := 0;
    V_PCT_CONSUMOS         NUMBER := 0;
    V_DESC_CONSUMOS_USD    NUMBER := 0;
    V_DESC_AGENCIA_USD     NUMBER := 0;
    V_TOTAL_USD            NUMBER := 0;
    V_MSG_ERROR            REG_ERRORES.MSG_ERROR%TYPE;

    CURSOR C_HUESPEDES_DIA IS
        SELECT H.ID_HUESPED,
               SUBSTR(H.APPAT_HUESPED || ' ' || H.APMAT_HUESPED || ' ' || H.NOM_HUESPED, 1, 60) AS NOMBRE,
               ROUND(
                   NVL(
                       SUM(
                           (NVL(HA.VALOR_HABITACION, 0) + NVL(HA.VALOR_MINIBAR, 0)) * R.ESTADIA
                       ),
                       0
                   )
               ) AS ALOJAMIENTO_USD
          FROM HUESPED H
          JOIN RESERVA R
            ON R.ID_HUESPED = H.ID_HUESPED
          LEFT JOIN DETALLE_RESERVA DR
            ON DR.ID_RESERVA = R.ID_RESERVA
          LEFT JOIN HABITACION HA
            ON HA.ID_HABITACION = DR.ID_HABITACION
         WHERE TRUNC(R.INGRESO + R.ESTADIA) = TRUNC(P_FECHA_PROCESO)
         GROUP BY H.ID_HUESPED,
                  SUBSTR(H.APPAT_HUESPED || ' ' || H.APMAT_HUESPED || ' ' || H.NOM_HUESPED, 1, 60)
         ORDER BY H.ID_HUESPED;
BEGIN
    IF NVL(P_VALOR_DOLAR, 0) <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'El valor del dolar debe ser mayor a cero.');
    END IF;

    -- Limpieza de tablas de salida para permitir multiples ejecuciones.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_DIARIO_HUESPEDES';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE REG_ERRORES';

    FOR R_H IN C_HUESPEDES_DIA LOOP
        BEGIN
            V_AGENCIA := FN_OBT_AGENCIA_HUESPED(R_H.ID_HUESPED);
            V_CONSUMOS_USD := FN_OBT_CONSUMOS_USD(R_H.ID_HUESPED);
            V_TOURS_USD := PKG_COBRANZA_HOTEL.FN_MONTO_TOURS_USD(R_H.ID_HUESPED);
            V_COBRO_PERSONA_USD := (V_COBRO_PERSONA_CLP * V_CANT_PERSONAS) / P_VALOR_DOLAR;

            -- Regla de negocio: subtotal acumulado = alojamiento + consumos + valor por persona.
            V_SUBTOTAL_USD := NVL(R_H.ALOJAMIENTO_USD, 0)
                            + NVL(V_CONSUMOS_USD, 0)
                            + V_COBRO_PERSONA_USD;

            SELECT NVL(MAX(TC.PCT), 0)
              INTO V_PCT_CONSUMOS
              FROM TRAMOS_CONSUMOS TC
             WHERE V_CONSUMOS_USD BETWEEN TC.VMIN_TRAMO AND TC.VMAX_TRAMO;

            V_DESC_CONSUMOS_USD := V_CONSUMOS_USD * V_PCT_CONSUMOS;

            IF UPPER(TRIM(V_AGENCIA)) = 'VIAJES ALBERTI' THEN
                V_DESC_AGENCIA_USD := V_SUBTOTAL_USD * 0.12;
            ELSE
                V_DESC_AGENCIA_USD := 0;
            END IF;

            V_TOTAL_USD := V_SUBTOTAL_USD - V_DESC_CONSUMOS_USD - V_DESC_AGENCIA_USD;
            V_TOTAL_USD := GREATEST(V_TOTAL_USD, 0);

            INSERT INTO DETALLE_DIARIO_HUESPEDES (
                ID_HUESPED,
                NOMBRE,
                AGENCIA,
                ALOJAMIENTO,
                CONSUMOS,
                TOURS,
                SUBTOTAL_PAGO,
                DESCUENTO_CONSUMOS,
                DESCUENTOS_AGENCIA,
                TOTAL
            ) VALUES (
                R_H.ID_HUESPED,
                R_H.NOMBRE,
                SUBSTR(V_AGENCIA, 1, 40),
                ROUND(R_H.ALOJAMIENTO_USD * P_VALOR_DOLAR),
                ROUND(V_CONSUMOS_USD * P_VALOR_DOLAR),
                ROUND(V_TOURS_USD * P_VALOR_DOLAR),
                ROUND(V_SUBTOTAL_USD * P_VALOR_DOLAR),
                ROUND(V_DESC_CONSUMOS_USD * P_VALOR_DOLAR),
                ROUND(V_DESC_AGENCIA_USD * P_VALOR_DOLAR),
                ROUND(V_TOTAL_USD * P_VALOR_DOLAR)
            );
        EXCEPTION
            WHEN OTHERS THEN
                V_MSG_ERROR := SQLERRM;
                INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
                VALUES (
                    SQ_ERROR.NEXTVAL,
                    SUBSTR('Error en SP COBRANZA al procesar huesped con id ' || R_H.ID_HUESPED, 1, 200),
                    SUBSTR(V_MSG_ERROR, 1, 300)
                );
        END;
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_PROCESA_COBRANZA_DIARIA;
/

-- ============================================================================
-- 6) PRUEBAS CASO 1 (TRIGGER)
-- ============================================================================
PROMPT
PROMPT ==========================================
PROMPT PRUEBAS CASO 1 - TRIGGER TOTAL_CONSUMOS
PROMPT ==========================================
PROMPT

DECLARE
    V_NEW_ID NUMBER;
    V_EXISTE NUMBER;
BEGIN
    -- Si quedaran duplicados de corridas anteriores, conserva solo uno.
    DELETE FROM CONSUMO C
     WHERE C.ID_RESERVA = 1587
       AND C.ID_HUESPED = 340006
       AND C.MONTO = 150
       AND C.ID_CONSUMO <> (
           SELECT MIN(C2.ID_CONSUMO)
             FROM CONSUMO C2
            WHERE C2.ID_RESERVA = 1587
              AND C2.ID_HUESPED = 340006
              AND C2.MONTO = 150
       );

    -- Evita duplicar el consumo de prueba si el script se ejecuta mas de una vez.
    SELECT COUNT(*)
      INTO V_EXISTE
      FROM CONSUMO
     WHERE ID_RESERVA = 1587
       AND ID_HUESPED = 340006
       AND MONTO = 150;

    IF V_EXISTE = 0 THEN
        SELECT MAX(ID_CONSUMO) + 1
          INTO V_NEW_ID
          FROM CONSUMO;

        INSERT INTO CONSUMO (ID_CONSUMO, ID_RESERVA, ID_HUESPED, MONTO)
        VALUES (V_NEW_ID, 1587, 340006, 150);
    END IF;

    DELETE FROM CONSUMO
     WHERE ID_CONSUMO = 11473;

    UPDATE CONSUMO
       SET MONTO = 95
     WHERE ID_CONSUMO = 10688;

    COMMIT;
END;
/

-- ============================================================================
-- 7) EJECUCION PROCEDIMIENTO PRINCIPAL
-- ============================================================================
DECLARE
    -- Parametros de ejecucion (puede cambiar estos valores antes de ejecutar).
    V_FECHA_PROCESO DATE := TO_DATE('18/08/2021', 'DD/MM/YYYY');
    V_VALOR_DOLAR   NUMBER := 915;
BEGIN
    SP_PROCESA_COBRANZA_DIARIA(
        V_FECHA_PROCESO,
        V_VALOR_DOLAR
    );
END;
/

-- ============================================================================
-- 8) CONSULTAS DE EVIDENCIA
-- ============================================================================
PROMPT
PROMPT ==========================================
PROMPT EVIDENCIA 1 - CONSUMOS AFECTADOS
PROMPT ==========================================
PROMPT

SELECT ID_CONSUMO, ID_RESERVA, ID_HUESPED, MONTO
  FROM CONSUMO
 WHERE ID_HUESPED IN (340003, 340004, 340006, 340008, 340009)
 ORDER BY  ID_CONSUMO;

PROMPT
PROMPT ==========================================
PROMPT EVIDENCIA 2 - TOTAL_CONSUMOS AFECTADOS
PROMPT ==========================================
PROMPT

SELECT ID_HUESPED, MONTO_CONSUMOS
  FROM TOTAL_CONSUMOS
 WHERE ID_HUESPED IN (340003, 340004, 340006, 340008, 340009)
 ORDER BY ID_HUESPED;

PROMPT
PROMPT ==========================================
PROMPT EVIDENCIA 3 - DETALLE_DIARIO_HUESPEDES
PROMPT ==========================================
PROMPT

SELECT ID_HUESPED,
       NOMBRE,
       AGENCIA,
       ALOJAMIENTO,
       CONSUMOS,
       SUBTOTAL_PAGO,
       DESCUENTO_CONSUMOS,
       DESCUENTOS_AGENCIA,
       TOTAL
  FROM DETALLE_DIARIO_HUESPEDES
 ORDER BY ID_HUESPED;

PROMPT
PROMPT ==========================================
PROMPT EVIDENCIA 4 - REG_ERRORES
PROMPT ==========================================
PROMPT

SELECT ROW_NUMBER() OVER (ORDER BY ID_ERROR) AS ID_ERROR,
       NOMSUBPROGRAMA,
       MSG_ERROR
  FROM REG_ERRORES
 ORDER BY ID_ERROR;

PROMPT
PROMPT ==========================================
PROMPT FIN SCRIPT SUMATIVA S8
PROMPT ==========================================
