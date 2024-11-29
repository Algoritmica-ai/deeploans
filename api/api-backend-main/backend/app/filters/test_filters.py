from .parser import parse_filter_query


def test_happy_path():
    filter_query = "-as15:pl and AS26:3"

    result = parse_filter_query(credit_type="sme", table_name="obligors", q=filter_query)
    assert result == [
        {
            'is_negated': True,
            'column': 'AS15', 
            'operator': '=',
            'value': 'PL',
            'value_type': 'STRING', 
            'logical_operator': 'AND'},
        {
            'is_negated': False,
            'column': 'AS26',
            'operator': '=',
            'value': '3',
            'value_type': 'STRING',
            'logical_operator': ''
        }
    ]
