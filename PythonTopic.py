def aggregate_payments(payments: list[dict]) -> dict:
    latest_payment={}

    for payment in payments:
        payment_id= payment["payment_id"]

        if payment_id not in latest_payment:
            latest_payment[payment_id]= payment

        else:
            existing_payment= latest_payment[payment_id]
            if (existing_payment['updated_date'],existing_payment['event_id']) < (payment['updated_date'],payment['event_id']):
                latest_payment[payment_id]= payment


    result ={}

    for payment in latest_payment.values():

        merchant_id= payment["merchant_id"]
        if payment['status']!="SUCCESS":
            continue

        if merchant_id not in result:
            result[merchant_id]= {'total_amount':0,'count':0}

        result[merchant_id]['total_amount']+= payment['amount']
        result[merchant_id]['count'] += 1


    return result
