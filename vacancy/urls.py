from django.urls import path
from vacancy.views import get_matched_vacancies_by_skill_set, get_statistics_salary_to_company, \
    get_statistics_salary_to_experience, get_top_10_skill_set


urlpatterns = [
    # path('api/', VacancyStatistics.as_view()),
    path('api/experience', get_statistics_salary_to_experience, name="salary-to-experience"),
    path('api/company', get_statistics_salary_to_company, name="salary-to-company"),
    path('api/vacancies', get_matched_vacancies_by_skill_set, name="vacancies-matched"),
    path('api/top-skills', get_top_10_skill_set, name="top-skills"),
]