class Budget:
    current_id = 1

    def __init__(self, category, amount, time_period):
        self.category = category
        self.amount = amount
        self.time_period = time_period
        self.id = Budget.current_id
        Budget.current_id+=1

    def __str__(self):
        return f"Category: {self.category}, Amount: {self.amount}, Time Period: {self.time_period}"
    def get_amount(self):
        return self.amount
    def get_cateogery(self):
        return self.category
    def get_time_period(self):
        return self.time_period
    def get_id(self):
        return self.id