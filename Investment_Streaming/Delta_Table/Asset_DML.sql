INSERT INTO dev.spark_db.asset (
    asset_external_id,
    asset_name,
    asset_currency,
    asset_status,
    asset_var,
    asset_dml
)
VALUES
('RELIANCE',   'Reliance Industries',  'INR', 1, 1, current_timestamp()),
('TCS',        'Tata Consultancy Services', 'INR', 1, 1, current_timestamp()),
('HDFCBANK',   'HDFC Bank',            'INR', 1, 1, current_timestamp()),
('BHARTIARTL', 'Bharti Airtel',        'INR', 1, 1, current_timestamp()),
('ICICIBANK',  'ICICI Bank',           'INR', 1, 1, current_timestamp()),
('NVDA',       'NVIDIA',               'USD', 1, 1, current_timestamp()),
('GOOGL',      'Alphabet Google',      'USD', 1, 1, current_timestamp()),
('AAPL',       'Apple',                'USD', 1, 1, current_timestamp()),
('MSFT',       'Microsoft',            'USD', 1, 1, current_timestamp()),
('AMZN',       'Amazon',               'USD', 1, 1, current_timestamp());
