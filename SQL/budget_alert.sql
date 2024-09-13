CREATE TABLE budgets (
    budget_id     NUMBER PRIMARY KEY,
    user_id      VARCHAR2(1000) NOT NULL,
    category      VARCHAR2(50),
    amount        NUMBER(10, 2),
    start_date    DATE,
    end_date      DATE,
    comments VARCHAR2(500)
);

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

INSERT INTO expenses (ids, user_id, amount, category_id, category_name, expense_date, descriptions, is_imported) VALUES
('EXP0001', 'USER001', 1000.00, 'CAT001', 'Travel', TIMESTAMP '2024-06-15 18:45:00', 'Team dinner', 0);

INSERT INTO expenses (ids, user_id, amount, category_id, category_name, expense_date, descriptions, is_imported) VALUES
('EXP0002', 'USER003', 500.00, 'CAT001', 'Marketing', TIMESTAMP '2024-06-15 18:45:00', 'Team dinner', 0);

INSERT INTO expenses (ids, user_id, amount, category_id, category_name, expense_date, descriptions, is_imported) VALUES
('EXP0003', 'USER003', 1800.00, 'CAT001', 'Marketing', TIMESTAMP '2024-06-15 18:45:00', 'Team dinner', 0);

-- Sample Budget 1
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date, comments)
VALUES (1, 'USER001', 'Travel', 1500.00, TO_DATE('2024-01-01', 'YYYY-MM-DD'), TO_DATE('2024-06-30', 'YYYY-MM-DD'), 'Under Control');

-- Sample Budget 2
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date, comments)
VALUES (2, 'USER002', 'Office Supplies', 500.00, TO_DATE('2024-02-01', 'YYYY-MM-DD'), TO_DATE('2024-12-31', 'YYYY-MM-DD'), 'Under Control');

-- Sample Budget 3
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date, comments)
VALUES (3, 'USER003', 'Marketing', 2000.00, TO_DATE('2024-03-01', 'YYYY-MM-DD'), TO_DATE('2024-09-30', 'YYYY-MM-DD'), 'Under Control');

-- Sample Budget 4
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date, comments)
VALUES (4, 'USER004', 'Training', 1200.00, TO_DATE('2024-05-01', 'YYYY-MM-DD'), TO_DATE('2024-12-31', 'YYYY-MM-DD'), 'Under Control');

-- Sample Budget 5
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date, comments)
VALUES (5, 'USER005', 'Equipment', 3000.00, TO_DATE('2024-01-15', 'YYYY-MM-DD'), TO_DATE('2024-08-15', 'YYYY-MM-DD'), 'Under Control');


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