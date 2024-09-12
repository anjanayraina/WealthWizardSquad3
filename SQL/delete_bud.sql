--drop table budgets
CREATE TABLE budgets (
    budget_id     NUMBER PRIMARY KEY,
    user_id       VARCHAR2(1000) NOT NULL,
    category      VARCHAR2(50),
    amount        NUMBER(10, 2),
    start_date    DATE,
    end_date      DATE
);

desc budgets;
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (1, 101, 'Groceries', 500.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (2, 102, 'Dining Out', 200.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (3, 101, 'Entertainment', 150.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (4, 103, 'Utilities', 300.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (5, 102, 'Transportation', 100.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

select *  from budgets;
--ALTER TABLE budgets 
--MODIFY (user_id VARCHAR2(50));

CREATE OR REPLACE PROCEDURE delete_budget_proc(p_budget_id IN NUMBER) IS
BEGIN
    DELETE FROM budgets WHERE budget_id = p_budget_id;
    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No budget found with ID ' || p_budget_id);
    ELSE
        DBMS_OUTPUT.PUT_LINE('Budget with ID ' || p_budget_id || ' deleted successfully.');
    END IF;
    
END;


-- Insert a test budget entry
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (999, 104, 'hobbies', 100.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

SELECT * FROM budgets;

BEGIN
    delete_budget_proc(2);
END;

drop PROCEDURE create_budget_proc;
CREATE OR REPLACE PROCEDURE create_budget_proc(
    p_budget_id IN NUMBER,
    p_user_id IN VARCHAR2,
    p_category IN VARCHAR2,
    p_amount IN NUMBER,
    p_start_date IN VARCHAR2,
    p_end_date IN VARCHAR2
) AS
BEGIN
    INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
    VALUES (
        p_budget_id,
        p_user_id,
        p_category,
        p_amount,
        TO_DATE(p_start_date, 'DD-MM-YYYY'),
        TO_DATE(p_end_date, 'DD-MM-YYYY')
    );
    
END;

BEGIN
    delete_budget(999);

END;

SELECT * FROM v$session WHERE blocking_session IS NOT NULL;
LOCK TABLE budgets IN EXCLUSIVE MODE;


DECLARE
  v_timeout NUMBER := 30; -- wait 60 seconds
BEGIN
  EXECUTE IMMEDIATE 'LOCK TABLE budgets IN EXCLUSIVE MODE WAIT ' || v_timeout;
END;