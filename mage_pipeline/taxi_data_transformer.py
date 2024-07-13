import pandas as pd
import pyarrow.parquet as pq
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # df = data.to_pandas()
    # print(data.head())
    # df = df.drop_duplicates().reset_index(drop=True)
    # df['trip_id'] = df.index

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input data is not a pandas DataFrame")

    print(df.shape)
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[['datetime_id','tpep_pickup_datetime', 'pickup_hour',
       'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
       'tpep_dropoff_datetime','drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday'
       ]]

    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True) 
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    rate_code_type = {1: 'Standard rate',
        2:'JFK',
        3:'Newark',
        4:'Nassau or Westchester',
        5:'Negotiated fare',
        6:'Group ride',
    }

    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]


    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    pickup_location_dim = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','PULocationID']]

    dropoff_location_dim = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','DOLocationID']]

    main_table = df.merge(payment_type_dim,on='payment_type') \
                .merge(datetime_dim,on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
                .merge(passenger_count_dim,on='passenger_count') \
                .merge(trip_distance_dim,on='trip_distance') \
                .merge(rate_code_dim,on='RatecodeID') \
                .merge(pickup_location_dim,on='PULocationID') \
                .merge(dropoff_location_dim,on='DOLocationID') \
                [['VendorID', 'datetime_id',
       'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id',
        'store_and_fwd_flag', 'payment_type_id', 'fare_amount', 'extra',
       'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
       'total_amount', 'congestion_surcharge', 'airport_fee']]

    print('Tranformation Done')

    # return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    # "passenger_count_dim":passenger_count_dim.to_dict(orient="dict"),
    # "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
    # "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
    # "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    # "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    # "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    # "main_table":main_table.to_dict(orient="dict")}
    # return {
    #     "datetime_dim": datetime_dim.to_dict(orient="dict"),
    #     "passenger_count_dim": passenger_count_dim.to_dict(orient="dict"),
    #     "trip_distance_dim": trip_distance_dim.to_dict(orient="dict"),
    #     "rate_code_dim": rate_code_dim.to_dict(orient="dict"),
    #     "pickup_location_dim": pickup_location_dim.to_dict(orient="dict"),
    #     "dropoff_location_dim": dropoff_location_dim.to_dict(orient="dict"),
    #     "payment_type_dim": payment_type_dim.to_dict(orient="dict"),
    #     "main_table": main_table.to_dict(orient="dict")
    # }
    return {
        "datetime_dim": datetime_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pickup_location_dim": pickup_location_dim,
        "dropoff_location_dim": dropoff_location_dim,
        "payment_type_dim": payment_type_dim,
        "main_table": main_table
    }



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    print(output)
    assert output is not None, 'The output is undefined'
