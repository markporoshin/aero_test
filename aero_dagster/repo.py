from dagster import repository

from .jobs import aero_el_job, aero_el_schedule

@repository()
def aero_repo():
    return [aero_el_job, aero_el_schedule]
