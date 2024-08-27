-- Inserting records into budgets
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (1, '101', 'Groceries', 500.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (2, '102', 'Dining Out', 200.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (3, '101', 'Entertainment', 150.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (4, '103', 'Utilities', 300.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (5, '102', 'Transportation', 100.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

-- Insert a test budget entry
INSERT INTO budgets (budget_id, user_id, category, amount, start_date, end_date)
VALUES (999, '104', 'hobbies', 100.00, TO_DATE('2024-08-01', 'YYYY-MM-DD'), TO_DATE('2024-08-31', 'YYYY-MM-DD'));

SELECT * FROM budgets;

BEGIN
    delete_budget(999);
END;



-- Test code for editing budget
BEGIN
    edit_budget(1,101,'Groceries',1000,TO_DATE('2024-08-01','YYYY-MM-DD'),TO_DATE('2024-08-30','YYYY-MM-DD'));
END;
/

