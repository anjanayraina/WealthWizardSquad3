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
CREATE OR REPLACE PROCEDURE view_all_budgets (
    p_user_id IN NUMBER,
    p_budget_id IN VARCHAR2,
    p_category OUT VARCHAR2,
    p_amount OUT NUMBER,
    p_start_date OUT DATE,
    p_end_date OUT DATE
) AS
BEGIN
    SELECT category, amount, start_date, end_date
    INTO p_category, p_amount, p_start_date, p_end_date
    FROM budgets
    WHERE user_id = p_user_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle the case where no data is found
        p_category := NULL;
        p_amount := NULL;
        p_start_date := NULL;
        p_end_date := NULL;
END;
/

--For Raising Budget Alerts
CREATE TABLE expenses (
    ids            VARCHAR2(1000) PRIMARY KEY,
    user_id       VARCHAR2(1000) NOT NULL,
    amount        NUMBER(10, 2) NOT NULL,
    category_id   VARCHAR2(100) NOT NULL,
    category_name VARCHAR2(100) NOT NULL,
    expense_date  TIMESTAMP NOT NULL,
    descriptions   VARCHAR2(255),
    is_imported   NUMBER(1)
);

CREATE OR REPLACE PROCEDURE check_budget_alerts AS
    CURSOR budget_cursor IS
        SELECT budget_id, user_id, category, amount, start_date, end_date
        FROM budgets;

    v_total_expenses NUMBER(10, 2);
    v_spending NUMBER(10, 2);
    v_amount NUMBER(10, 2);
    v_comment VARCHAR2(500);
    v_budget_id NUMBER;
    v_user_id VARCHAR2(1000);
    v_category VARCHAR2(50);
BEGIN
    FOR budget_rec IN budget_cursor LOOP
        v_budget_id := budget_rec.budget_id;
        v_user_id := budget_rec.user_id;
        v_category := budget_rec.category;
        v_amount := budget_rec.amount;

        DBMS_OUTPUT.PUT_LINE('Processing Budget ID: ' || v_budget_id);
        DBMS_OUTPUT.PUT_LINE('User ID: ' || v_user_id);
        DBMS_OUTPUT.PUT_LINE('Category: ' || v_category);
        DBMS_OUTPUT.PUT_LINE('Amount: ' || v_amount);

        -- Calculate total expenses for this budget and category
        BEGIN
            SELECT NVL(SUM(e.amount), 0)
            INTO v_total_expenses
            FROM expenses e
            WHERE e.user_id = v_user_id
              AND e.category_name = v_category
              AND e.expense_date BETWEEN budget_rec.start_date AND budget_rec.end_date;

            -- Determine the spending status
            v_spending := v_total_expenses;

            IF v_spending >= v_amount THEN
                v_comment := 'Budget exceeded!';
            ELSIF v_spending >= v_amount * 0.9 THEN
                v_comment := 'Budget limit approaching!';
            ELSE
                v_comment := 'Budget is under control.';
            END IF;

            -- Update the comment in the budgets table
            BEGIN
                UPDATE budgets
                SET comments = v_comment
                WHERE budget_id = v_budget_id;

                DBMS_OUTPUT.PUT_LINE('Updated budget ID ' || v_budget_id || ' with comment: ' || v_comment);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error updating budget ID ' || v_budget_id || ': ' || SQLERRM);
            END;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error calculating expenses for user ID ' || v_user_id || ' and category ' || v_category || ': ' || SQLERRM);
        END;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error in procedure: ' || SQLERRM);
END;
/
