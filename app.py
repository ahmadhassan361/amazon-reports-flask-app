import json
from flask import Flask, request, jsonify
import time
from io import BytesIO, StringIO
from datetime import datetime, timedelta
import pandas as pd
from sp_api.api import Reports
from sp_api.base import ReportType, ProcessingStatus
from sp_api.base.exceptions import SellingApiRequestThrottledException
from sp_api.base import Marketplaces
import gzip
import requests
app = Flask(__name__)
marketplaces_dict = {
    "US": Marketplaces.US,
    "AE": Marketplaces.AE,
    "BE": Marketplaces.BE,
    "DE": Marketplaces.DE,
    "PL": Marketplaces.PL,
    "EG": Marketplaces.EG,
    "ES": Marketplaces.ES,
    "FR": Marketplaces.FR,
    "GB": Marketplaces.GB,
    "IN": Marketplaces.IN,
    "IT": Marketplaces.IT,
    "NL": Marketplaces.NL,
    "SA": Marketplaces.SA,
    "SE": Marketplaces.SE,
    "TR": Marketplaces.TR,
    "UK": Marketplaces.UK,
    "ZA": Marketplaces.ZA,
    "AU": Marketplaces.AU,
    "JP": Marketplaces.JP,
    "SG": Marketplaces.SG,
    "BR": Marketplaces.BR,
    "CA": Marketplaces.CA,
    "MX": Marketplaces.MX
}
def get_24_hours_ago():
    current_time = datetime.now()
    time_24_hours_ago = current_time - timedelta(hours=24)
    data_start_time = time_24_hours_ago.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return data_start_time

def get_60_days_ago():
    current_time = datetime.now()
    time_60_days_ago = current_time - timedelta(days=30)
    data_start_time = time_60_days_ago.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return data_start_time

def get_skus_report(credentials, report_type, market, start_date=None, end_date=None):
    try:
        reports_api = Reports(credentials=credentials, marketplace=market)
        report_id = None
        while report_id is None:
            try:
                if start_date and end_date:
                    create_response = reports_api.create_report(reportType=report_type, dataStartTime=start_date,dataEndTime=end_date)
                elif start_date:
                    create_response = reports_api.create_report(reportType=report_type, dataStartTime=start_date)
                else:
                    create_response = reports_api.create_report(reportType=report_type)
                
                report_id = create_response.payload["reportId"]
            except SellingApiRequestThrottledException:
                time.sleep(5)

        get_response = None
        while get_response is None:
            try:
                get_response = reports_api.get_report(report_id)
            except:
                time.sleep(5)

        while get_response.payload.get("processingStatus") != ProcessingStatus.DONE:
            time.sleep(5)
            try:
                get_response = reports_api.get_report(report_id)
            except SellingApiRequestThrottledException:
                time.sleep(5)
                continue
            if get_response.payload.get("processingStatus") in [ProcessingStatus.FATAL, ProcessingStatus.CANCELLED]:
                raise RuntimeError("report failed")

        report_data = None
        while report_data is None:
            try:
                report_data = reports_api.get_report_document(get_response.payload["reportDocumentId"], download=True)
            except SellingApiRequestThrottledException:
                time.sleep(5)
        print(report_data.payload.get("url"))
        response = requests.get(report_data.payload.get("url"))

        if report_type in ["RFQD_BULK_DOWNLOAD", "FEE_DISCOUNTS_REPORT"]:
            try:
                decoded_str = gzip.decompress(response.content)
            except:
                decoded_str = response.content
            excel_data = pd.read_excel(BytesIO(decoded_str))
            return excel_data.to_json(orient="records")
        else:
            try:
                decoded_str = gzip.decompress(response.content).decode("utf-8")
                print(decoded_str)
            except:
                decoded_str = response.content.decode("utf-8", errors="replace")
                # print(decoded_str)
                # print("ext")
            if report_type == "GET_SALES_AND_TRAFFIC_REPORT":
                return json.loads(decoded_str)
            return json.loads(pd.read_csv(StringIO(decoded_str), sep="\t").to_json(orient="records"))
    except Exception as e:
        return {"error": str(e)}

@app.route('/generate_report', methods=['POST'])
def generate_report():
    try:
        print("runner")
        body = request.json
        credentials = body["credentials"]
        report_type = body["report_type"]
        marketplace = body["marketplace"]
        start_date = body.get("start_date",None)
        end_date = body.get("end_date",None)

        result_json = get_skus_report(credentials, report_type, marketplaces_dict[marketplace],start_date=start_date,end_date=end_date)

        return jsonify(result_json), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__  == '__main__':
    app.run(debug=True)