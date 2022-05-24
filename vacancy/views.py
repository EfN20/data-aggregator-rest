from django.http import HttpResponse
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

import json
import os.path

import requests
import pandas as pd
import csv
import os.path as path
import time
import threading

from numpy import isnan

URL_LIST_VACANCIES = "https://api.hh.ru/vacancies"
EXPERIENCE_DICTIONARY = {
    'noExperience': '0',
    'between1And3': '1-3',
    'between3And6': '3-6',
    'moreThan6': '6+'
}
pd.set_option('display.float_format', lambda x: '%.f' % x)


class Vacancy:
    def __init__(self, id, name, area, experience, company_name, company_logo_url,
                 salary_from, salary_to, skill_set, match_by_skill_set, link_to_vacancy):
        self.id = id
        self.name = name
        self.area = area
        self.experience = experience
        self.company_name = company_name
        self.company_logo_url = company_logo_url
        self.salary_from = salary_from
        self.salary_to = salary_to
        self.skill_set = skill_set
        self.match_by_skill_set = match_by_skill_set
        self.link_to_vacancy = link_to_vacancy


class Skill:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class MyEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


class CurrencyConvertor:
    rates = {}

    def __init__(self, url):
        data = requests.get(url).json()
        self.rates = data["rates"]

    def convert(self, from_currency, to_currency, amount):
        initial_amount = amount
        if from_currency != 'EUR':
            amount = amount / self.rates[from_currency]
        amount = round(amount * self.rates[to_currency], 2)
        print('{} {} = {} {}'.format(initial_amount, from_currency, amount, to_currency))


def collecting_data_in_page(shared_list, base_url, page):
    url_list_page = base_url + "&page=" + str(page)
    new_req = requests.get(url=url_list_page)
    new_data = new_req.json()
    items = new_data['items']
    for item in items:
        try:
            single_vacancy_req = requests.get(url=URL_LIST_VACANCIES + '/' + item['id'])
            single_vacancy_data = single_vacancy_req.json()
            if single_vacancy_data['salary'] is not None:
                single_vacancy_data['salary_from'] = None \
                    if single_vacancy_data['salary']['from'] is None else single_vacancy_data['salary']['from']
                single_vacancy_data['salary_to'] = None \
                    if single_vacancy_data['salary']['to'] is None else single_vacancy_data['salary']['to']

                if single_vacancy_data['salary']['currency'] == 'RUR':
                    single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 4.7
                    single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 4.7

                if single_vacancy_data['salary']['currency'] == 'USD':
                    single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 446
                    single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 446

                if single_vacancy_data['salary']['currency'] == 'EUR':
                    single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 466
                    single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 466
            else:
                single_vacancy_data['salary_from'] = None
                single_vacancy_data['salary_to'] = None

            single_vacancy_data.pop("branded_description", None)
            shared_list.append(single_vacancy_data)
            if 'languages' not in single_vacancy_data:
                single_vacancy_data['languages'] = ""
        except Exception:
            print(f"Something happened with vacancy ${item['id']}")


def create_csv_of_all_vacancies_in_area(word_to_find=None, area=159):
    if is_file_older_than_12_hours("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv"):
        start_time = time.time()
        url_list_vacancies_in_area = URL_LIST_VACANCIES + "?area=" + str(area) + "&text=" + str(word_to_find)
        if word_to_find is None:
            url_list_vacancies_in_area = URL_LIST_VACANCIES + "?area=" + str(area)
        req = requests.get(url=url_list_vacancies_in_area)
        data = req.json()
        pages = data['pages']
        result = []
        threads = []
        for i in range(pages):
            thread = threading.Thread(target=collecting_data_in_page(result, url_list_vacancies_in_area, i))
            threads.append(thread)
            thread.start()
            # url_list_page = url_list_vacancies_in_area + "&page=" + str(i)
            # new_req = requests.get(url=url_list_page)
            # new_data = new_req.json()
            # items = new_data['items']
            # for item in items:
            #     try:
            #         single_vacancy_req = requests.get(url=URL_LIST_VACANCIES + '/' + item['id'])
            #         single_vacancy_data = single_vacancy_req.json()
            #         if single_vacancy_data['salary'] is not None:
            #             single_vacancy_data['salary_from'] = None \
            #                 if single_vacancy_data['salary']['from'] is None else single_vacancy_data['salary']['from']
            #             single_vacancy_data['salary_to'] = None \
            #                 if single_vacancy_data['salary']['to'] is None else single_vacancy_data['salary']['to']
            #
            #             if single_vacancy_data['salary']['currency'] == 'RUR':
            #                 single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 4.7
            #                 single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 4.7
            #
            #             if single_vacancy_data['salary']['currency'] == 'USD':
            #                 single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 446
            #                 single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 446
            #
            #             if single_vacancy_data['salary']['currency'] == 'EUR':
            #                 single_vacancy_data['salary_from'] = single_vacancy_data['salary']['from'] * 466
            #                 single_vacancy_data['salary_to'] = single_vacancy_data['salary']['to'] * 466
            #         else:
            #             single_vacancy_data['salary_from'] = None
            #             single_vacancy_data['salary_to'] = None
            #
            #         single_vacancy_data.pop("branded_description", None)
            #         result.append(single_vacancy_data)
            #         if 'languages' not in single_vacancy_data:
            #             single_vacancy_data['languages'] = ""
            #     except Exception:
            #         print(f"Something happened with vacancy ${item['id']}")
        for thread in threads:
            thread.join()
        keys = result[0].keys()

        with open("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv", "w",
                  encoding="utf-8-sig", newline='') as a_file:
            dict_writer = csv.DictWriter(a_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(result)
            a_file.close()
            print("TIME: ", (time.time() - start_time))
    else:
        pass


def is_file_older_than_12_hours(file):
    if os.path.exists(file):
        file_time = path.getmtime(file)
        return (time.time() - file_time) / 3600 > 12
    else:
        return True


def create_analyzed_data_for_salary_to_experience(area=159, word_to_find=None):
    df = pd.read_csv("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv")
    new_col = df.loc[:, 'salary_from': 'salary_to']
    df['salary_mean'] = new_col.mean(axis=1)
    new_df = df.groupby('experience').salary_mean.mean()
    new_df.fillna(0, inplace=True)
    analyzed_data_dict = new_df.to_dict()
    keys_dict = analyzed_data_dict.keys()
    to_send_data_list = []
    for key in keys_dict:
        key = key.replace("\'", "\"")
        key = json.loads(key)
        to_send_data_dict_2 = {"year": EXPERIENCE_DICTIONARY[key["id"]], "salary": int(analyzed_data_dict[str(key)])}
        to_send_data_list.append(to_send_data_dict_2)

    to_send_data_list = sorted(to_send_data_list, key=lambda x: x["year"])
    to_send_json_2 = {"name": "Average salary to experience by search word " + word_to_find + " in KZT",
                      "data": to_send_data_list}
    print(json.dumps(to_send_json_2, indent=4))
    return to_send_json_2


def dict_clean(items):
    result = {}
    for key, value in items:
        if value is None:
            value = 'default'
        result[key] = value
    return result


def create_analyzed_data_for_salary_to_company(area=159, word_to_find=None):
    df = pd.read_csv("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv")
    new_col = df.loc[:, 'salary_from': 'salary_to']
    df['salary_mean'] = new_col.mean(axis=1)
    new_df = df.groupby('employer').salary_mean.mean()
    new_df.fillna(0, inplace=True)
    analyzed_data_dict = new_df.to_dict()
    clean_analyzed_data_dict = {k: analyzed_data_dict[k] for k in analyzed_data_dict if
                                not isnan(analyzed_data_dict[k])}
    keys_dict = clean_analyzed_data_dict.keys()
    to_send_data_list = []
    for key in keys_dict:
        json_key = key.replace("\'", "\"")
        json_key = json_key.replace('True', 'true')
        json_key = json_key.replace('False', 'false')
        json_key = json_key.replace('None', '\"default\"')
        json_key = json.loads(json_key)
        to_send_data = {"company_name": json_key['name'], "salary": int(clean_analyzed_data_dict[str(key)])}
        to_send_data_list.append(to_send_data)

    to_send_data_list = sorted(to_send_data_list, key=lambda x: x["salary"], reverse=True)
    to_send_json_2 = {"name": "Average salary to company by search word " + word_to_find + " in KZT",
                      "data": to_send_data_list}
    return to_send_json_2


def search_vacancies_by_skill_sets(skill_sets, area=159, word_to_find=None):
    df = pd.read_csv("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv")
    vacancies_sorted_by_skill_sets_match = []
    for index, row in df.iterrows():
        vacancy_skill_sets_row = row['key_skills']
        vacancy_skill_sets_row = vacancy_skill_sets_row.replace("\'", "\"")
        vacancy_skill_sets_row = json.loads(vacancy_skill_sets_row)
        match_by_skill_sets = 0
        vacancy_skill_sets = []
        if len(vacancy_skill_sets_row) == 0:
            pass
        else:
            for skill in vacancy_skill_sets_row:
                vacancy_skill_sets.append(skill['name'])
            match_by_skill_sets = len(set(vacancy_skill_sets) & set(skill_sets)) / float(len(vacancy_skill_sets)) * 100
            match_by_skill_sets = "{:.2f}".format(match_by_skill_sets)

        area_of_vacancy = row['area']
        area_of_vacancy = area_of_vacancy.replace("\'", "\"")
        area_of_vacancy = json.loads(area_of_vacancy)

        experience_of_vacancy = row['experience']
        experience_of_vacancy = experience_of_vacancy.replace("\'", "\"")
        experience_of_vacancy = json.loads(experience_of_vacancy)

        company_of_vacancy = row['employer']
        company_of_vacancy = company_of_vacancy.replace("\'", "\"")
        company_of_vacancy = company_of_vacancy.replace('True', 'true')
        company_of_vacancy = company_of_vacancy.replace('False', 'false')
        company_of_vacancy = company_of_vacancy.replace('None', '\"default\"')
        # print("AAAA", company_of_vacancy)
        company_of_vacancy = json.loads(company_of_vacancy)
        company_of_vacancy_logo_url = "no_logo"
        if 'logo_urls' in company_of_vacancy and company_of_vacancy['logo_urls'] != 'default':
            company_of_vacancy_logo_url = company_of_vacancy['logo_urls']['original']

        link_vacancy = row['alternate_url']

        vacancy = Vacancy(
            id=index,
            name=row['name'],
            area=area_of_vacancy['name'],
            experience=EXPERIENCE_DICTIONARY[experience_of_vacancy['id']],
            company_name=company_of_vacancy['name'],
            company_logo_url=company_of_vacancy_logo_url,
            salary_from=None if isnan(row['salary_from']) else row['salary_from'],
            salary_to=None if isnan(row['salary_to']) else row['salary_to'],
            skill_set=vacancy_skill_sets,
            match_by_skill_set=float(match_by_skill_sets),
            link_to_vacancy=link_vacancy
        )
        vacancy_to_send = json.dumps(vacancy, indent=4, ensure_ascii=False, cls=MyEncoder)
        vacancies_sorted_by_skill_sets_match.append(vacancy)

    vacancies_sorted_by_skill_sets_match.sort(key=lambda x: x.match_by_skill_set, reverse=True)
    return vacancies_sorted_by_skill_sets_match


def get_paginated_list(item_list, page, items_per_page):
    if page is None:
        page = 0
    if items_per_page is None:
        items_per_page = 10
    offset = page * items_per_page
    total_page = (len(item_list) // items_per_page)
    if len(item_list) % items_per_page != 0:
        total_page += 1
    result = {
        "data": item_list[offset:offset+items_per_page],
        "page": page,
        "totalPage": total_page,
        "itemsPerPage": items_per_page
    }
    return result


def top_10_skills(area=159, word_to_find=None):
    df = pd.read_csv("output" + "_word_" + str(word_to_find) + "_area_" + str(area) + ".csv")
    skills_occurrence = {}
    for index, row in df.iterrows():
        vacancy_skill_sets_row = row['key_skills']
        vacancy_skill_sets_row = vacancy_skill_sets_row.replace("\'", "\"")
        vacancy_skill_sets_row = json.loads(vacancy_skill_sets_row)
        if len(vacancy_skill_sets_row) == 0:
            pass
        else:
            for skill in vacancy_skill_sets_row:
                # print(skill)
                # print(type(skill))
                if skill['name'] in skills_occurrence:
                    skills_occurrence[skill['name']] += 1
                else:
                    skills_occurrence[skill['name']] = 1
    top_10_skills_dict = dict(sorted(skills_occurrence.items(), key=lambda x: x[1], reverse=True)[:10])
    print(len(df.index))
    to_send_data = []
    for key in top_10_skills_dict:
        single_data = {"skillName": key, "occurrence": top_10_skills_dict[key]}
        to_send_data.append(single_data)

    to_send_json = {"name": "Top 10 skills by search word " + word_to_find, "data": to_send_data,
                    "totalVacancies": len(df.index)}
    return to_send_json


@api_view(['POST'])
def get_statistics_salary_to_experience(request):
    area = request.data.get('area')
    word_to_find = request.data.get('wordToFind')
    if area and word_to_find:
        create_csv_of_all_vacancies_in_area(word_to_find, area)
        response_data = create_analyzed_data_for_salary_to_experience(area, word_to_find)
        return Response(
            data=response_data,
            status=status.HTTP_200_OK,
            content_type="application/json"
        )
    else:
        Response(
            {
                'status': 'bad request',
                'message': 'Area / wordToFind were not provided'
            },
            status=status.HTTP_400_BAD_REQUEST
        )


@api_view(['POST'])
def get_statistics_salary_to_company(request):
    area = request.data.get('area')
    word_to_find = request.data.get('wordToFind')
    if area and word_to_find:
        create_csv_of_all_vacancies_in_area(word_to_find, area)
        response_data = create_analyzed_data_for_salary_to_company(area, word_to_find)
        return Response(
            data=response_data,
            status=status.HTTP_200_OK,
            content_type="application/json"
        )
    else:
        Response(
            {
                'status': 'bad request',
                'message': 'Area / wordToFind were not provided'
            },
            status=status.HTTP_400_BAD_REQUEST
        )


@api_view(['POST'])
def get_matched_vacancies_by_skill_set(request):
    area = request.data.get('area')
    word_to_find = request.data.get('wordToFind')
    page = request.data.get('page')
    items_per_page = request.data.get('itemsPerPage')
    skill_set = request.data.get('skillSet')
    if area and word_to_find and skill_set:
        create_csv_of_all_vacancies_in_area(word_to_find, area)
        response_data = search_vacancies_by_skill_sets(skill_set, area, word_to_find)
        paginated_result = get_paginated_list(response_data, page, items_per_page)

        data_to_send = {
            "vacancies": json.loads(json.dumps(paginated_result['data'], ensure_ascii=False, cls=MyEncoder)),
            "page": paginated_result['page'],
            "totalPage": paginated_result['totalPage'],
            "itemsPerPage": paginated_result['itemsPerPage']
        }

        return Response(
            data=data_to_send,
            status=status.HTTP_200_OK,
            content_type="application/json"
        )
    else:
        Response(
            {
                'status': 'bad request',
                'message': 'Area / wordToFind / skillSet were not provided'
            },
            status=status.HTTP_400_BAD_REQUEST
        )


@api_view(['POST'])
def get_top_10_skill_set(request):
    area = request.data.get('area')
    word_to_find = request.data.get('wordToFind')
    if area and word_to_find:
        create_csv_of_all_vacancies_in_area(word_to_find, area)
        response_data = top_10_skills(area, word_to_find)
        return Response(
            data=response_data,
            status=status.HTTP_200_OK,
            content_type="application/json"
        )
    else:
        Response(
            {
                'status': 'bad request',
                'message': 'Area / wordToFind were not provided'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

