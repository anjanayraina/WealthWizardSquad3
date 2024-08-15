from datetime import datetime

class Budget:
    def __init__(self, budget_id, user_id, category, amount, start_date, end_date):
        self.budget_id = budget_id
        self.user_id = user_id
        self.category = category
        self.amount = amount
        self.start_date = datetime.strptime(start_date, '%d-%m-%Y')
        self.end_date = datetime.strptime(end_date, '%d-%m-%Y')

    def __repr__(self):
        return (f"Budget(budget_id={self.budget_id}, user_id={self.user_id}, category={self.category}, "
                f"amount={self.amount}, start_date={self.start_date.strftime('%d-%m-%Y')}, "
                f"end_date={self.end_date.strftime('%d-%m-%Y')})")
