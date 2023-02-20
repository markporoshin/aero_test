from dagster import op, job, In, schedule, RunRequest

from ..resources import rest_extractor, postgres_loader


@op(required_resource_keys={"extractor"})
def extract(context):
    return context.resources.extractor.extract()


@op(required_resource_keys={"loader"},
       ins={"data_df": In()})
def load(context, data_df):
    context.resources.loader.load(data_df)


@job(resource_defs={
    "extractor": rest_extractor,
    "loader": postgres_loader
})
def aero_el_job():
    load(data_df=extract())


@schedule(cron_schedule="0 */12 * * *", job=aero_el_job)
def aero_el_schedule(context):
    yield RunRequest(run_key=context.cursor)
