import ast
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class Item_Filter(beam.DoFn):                           # class to get the order_items separated from rest of data
    def process(self, element):
        needed_fields = ["order_id", "order_items"]
        new_group = {i: element[i] for i in needed_fields}  # making new, reduced order records

        yield new_group


# class to extract the item id and create the items list
class Order_Segment(beam.DoFn):
    def process(self, element):
        item_list = []

        for key, value in element.items():                        # nested for loop to iterate thru the order_items dict
            if key == "order_items":                                # another opportunity to simplify
                for sub_elememnts in value:
                    if isinstance(sub_elememnts, dict):             # can I get by w/out this since I know it's a dict?
                        for key1, value1 in sub_elememnts.items():
                            if key1 == "id":                        # selecting the id value
                                item_list.append(value1)

        element["items_list"] = item_list
        del element["order_items"]                          #do this to get rid of order_items and replace with the
        yield element                                       #item list. order_id and items_list remain


class bq_filter(beam.DoFn):                             # selecting the fields needed for BigQuery tables
    def process(self, element):
        new_group = {}
        needed_fields = ["customer_first_name", "customer_last_name", "customer_ip", "order_id", "order_address",
                         "cost_total"]
        new_group = {i: element[i] for i in needed_fields}          # making new, reduced order records

        yield new_group


def usd(element):                                                  # 3 functions to separate by currency
    return element["order_currency"] == "USD"


def gbp(element):
    return element["order_currency"] == "GBP"


def eur(element):
    return element["order_currency"] == "EUR"


# This class is to calculate the total cost of item price, tax and shipping


class TotalCost(beam.DoFn):
    def process(self, element):
        total_price = 0.0  # local variable to sum costs

        total_price += element["cost_tax"]
        total_price += element["cost_shipping"]

        for item in element["order_items"]:  # for loop to iterate thru the order_items dict
            total_price += item["price"]

        element["cost_total"] = round(total_price, 2)

        # print(element)
        yield element


class NameSplit(beam.DoFn):
    new_dict = {}

    def process(self, element):
        # print(element)
        # print(type(element))

        full_name = element["customer_name"].split(" ")
        customer_first_name = full_name[0]                                  # splitting the first and last names
        customer_last_name = full_name[1]

        order_building_number = element["order_address"].split(" ")[0]      # getting the bldg number
        street_addr = element["order_address"].split(",", -1)               # interim step to the wanted list element
        order_street_name = ""                                              # declaring variables before use in if stmnt
        order_city = ""
        order_state_code = ""
        order_zip_code = ""

        if len(street_addr) == 3:
            order_street_name = street_addr[0].split(" ", 1)[1]                 # working from interim step to get city,
            order_city = street_addr[1].strip()                                 # state and zip code
            order_state_code = street_addr[2].strip().split(" ")[0]
            order_zip_code = street_addr[2].split()[1]

        elif len(street_addr) < 3:
            pass

        new_dict = {
            "customer_first_name": customer_first_name, "customer_last_name": customer_last_name, "order_address":
                [{"order_building_num": order_building_number, "order_street_name": order_street_name, "order_city":
                 order_city, "order_state_code": order_state_code, "order_zip_code": order_zip_code}], "order_id":
                int(element["order_id"]), "customer_ip": element["customer_ip"], "order_items": element["order_items"],
                "cost_shipping": element["cost_shipping"], "cost_tax": element["cost_tax"], "order_currency":
                element["order_currency"]

        }
        print(new_dict)
        yield new_dict


class Reformat(beam.DoFn):
    def process(self, element):
        element_2 = element.decode("utf-8")
        element_3 = ast.literal_eval(element_2)

        yield element_3


topic_id = "projects/york-cdf-start/topics/dataflow-order-stock-update"         # topic for WriteToPubSub

if __name__ == '__main__':

    # Schema definition for all of the payment order history tables
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'Repeated',
                'fields': [
                    {'name': 'order_building_num', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_street_name', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_city', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_state_code', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_zip_code', 'type': 'STRING', 'mode': 'nullable'},
            ],
            },

            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'cost_total', 'type': 'Float', 'mode': 'nullable'},
        ]
    }

    # Table specification for the BQ tables
    table_usd_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='usd_order_payment_history'
    )

    table_gbp_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='gbp_order_payment_history'
    )

    table_eur_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='eur_order_payment_history'
    )

    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as pipeline:
        orders = pipeline | beam.io.ReadFromPubSub(subscription='projects/york-cdf-start/subscriptions/order_topic-sub')

        as_dict = orders | beam.ParDo(Reformat())           # Changes output from Bytes type to dict

        names = as_dict | beam.ParDo(NameSplit())           # Doing all name and address splits in one ParDoFn

        total_cost = names | beam.ParDo(TotalCost())        # calculating the combined cost

        # separating the orders by currency
        usd_split = total_cost | beam.Filter(usd)          #USD orders

        gbp_split = total_cost | beam.Filter(gbp)          # GBP orders

        eur_split = total_cost | beam.Filter(eur)          # Eur orders

        # Reducing down to just the necessary fields for writes to BigQuery
        USD_Orders = usd_split | "Filter for US" >> beam.ParDo(bq_filter())

        GBP_Orders = gbp_split | "Filter for GB" >> beam.ParDo(bq_filter())

        EUR_Orders = eur_split | "Filter for EU" >> beam.ParDo(bq_filter())

        # Getting the order_id and order_items data needed for PubSub together
        filtered_items = total_cost | beam.ParDo(Item_Filter())

        # making the items_list for PubSub
        items_list = filtered_items | beam.ParDo(Order_Segment())

        # changing from PColl back to JSON obj (something to be simplified later?)
        itemsList_orderID = items_list | beam.Map(lambda s: json.dumps(s).encode("utf-8"))

        itemsList_orderID | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=topic_id)

        # Outputting the tables to BigQuery.
        USD_Orders | "Write1" >> beam.io.WriteToBigQuery(
            table_usd_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )

        GBP_Orders | "Write2" >> beam.io.WriteToBigQuery(
            table_gbp_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"

        )

        EUR_Orders | "Write3" >> beam.io.WriteToBigQuery(
            table_eur_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )
