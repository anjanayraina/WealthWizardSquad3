-- Create budgets table
CREATE TABLE budgets (
    budget_id     NUMBER PRIMARY KEY,
    user_id       VARCHAR2(1000) NOT NULL,
    category      VARCHAR2(50),
    amount        NUMBER(10, 2),
    start_date    DATE,
    end_date      DATE
);

-- Creating a budget
CREATE OR REPLACE PROCEDURE create_budget_proc(
    p_budget_id IN budgets.budget_id%TYPE,
    p_user_id IN budgets.user_id%TYPE,
    p_category IN budgets.category%TYPE,
    p_amount IN budgets.amount%TYPE,
    p_start_date IN budgets.start_date%TYPE,
    p_end_date IN budgets.end_date%TYPE
)
IS BEGIN
    INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (p_budget_id, p_user_id, p_category, p_amount, TO_DATE(p_start_date, 'DD-MM-YYYY'), TO_DATE(p_end_date, 'DD-MM-YYYY'));
END create_budget_proc;
/

-- Editing a budget
CREATE OR REPLACE PROCEDURE edit_budget_proc(
    p_budget_id IN budgets.budget_id%TYPE,
    p_user_id IN budgets.user_id%TYPE,
    p_category IN budgets.category%TYPE,
    p_amount IN budgets.amount%TYPE,
    p_start_date IN budgets.start_date%TYPE,
    p_end_date IN budgets.end_date%TYPE
)
IS BEGIN
    UPDATE budgets
    SET budgets.category = p_category, budgets.amount = p_amount, budgets.start_date = TO_DATE(p_start_date,'DD-MM-YYYY'), budgets.end_date = TO_DATE(p_end_date,'DD-MM-YYYY')
    WHERE budgets.budget_id = p_budget_id AND budgets.user_id = p_user_id;
END edit_budget_proc;
/

-- Deleting budget procedure
CREATE OR REPLACE PROCEDURE delete_budget(p_budget_id IN NUMBER) IS
BEGIN
    DELETE FROM budgets WHERE budget_id = p_budget_id;
    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No budget found with ID ' || p_budget_id);
    ELSE
        DBMS_OUTPUT.PUT_LINE('Budget with ID ' || p_budget_id || ' deleted successfully.');
    END IF;
    
END;

-- Viewing all budgets
CREATE OR REPLACE PROCEDURE view_all_budgets
IS
BEGIN
    FOR rec IN (SELECT budget_id, category, amount, start_date, end_date FROM budgets)
    LOOP
        DBMS_OUTPUT.PUT_LINE('Budget ID: ' || rec.budget_id);
        DBMS_OUTPUT.PUT_LINE('Category: ' || rec.category);
        DBMS_OUTPUT.PUT_LINE('Amount: ' || rec.amount);
        DBMS_OUTPUT.PUT_LINE('Start Date: ' || rec.start_date);
        DBMS_OUTPUT.PUT_LINE('End Date: ' || rec.end_date);
        DBMS_OUTPUT.PUT_LINE('------------------------------------');
    END LOOP;
END view_all_budgets;
/

