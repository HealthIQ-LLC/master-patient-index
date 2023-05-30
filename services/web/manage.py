import click
from flask.cli import FlaskGroup
from project import app, db, COUPLER, Auditor

cli = FlaskGroup(app)


@cli.command("create_db")
def create_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


@cli.command('post')
@click.argument('endpoint')
@click.option('--user', default="CLI", 
    help='named system user')
@click.option('--test', default=False, 
    help='supply dummy demographic records for testing')
@click.option('--s3_uri', default=None, 
    help='location of demographics records')
@click.option('--record_id_low', default=None, 
    help='targeted demographic record')
@click.option('--record_id_high', default=None, 
    help='targeted demographic record')
@click.option('--record_id', default=None, 
    help='targeted demographic record')
@click.option('--proc_id', default=None, 
    help='targeted process key')
@click.option('--batch_id', default=None, 
    help='targeted batch key')
@click.option('--action', default=None, 
    help='a named behavior')
def empi_post(
    endpoint, 
    action, 
    batch_id, 
    proc_id, 
    record_id, 
    record_id_high, 
    record_id_low, 
    s3_uri, 
    test, 
    user
    ):
    payload = dict()
    if endpoint == 'demographic':
        if test:
            payload['demographics'] = list()
        else:
            # ToDo: read records from s3 uri into list here.
            payload['demographics'] = s3_uri  
    elif endpoint == 'delete_action':
        payload['batch_id'] = batch_id
        payload['proc_id'] = proc_id
        payload['action'] = action
    elif endpoint in (
        'activate_demographic', 
        'deactivate_demographic', 
        'delete_demographic'
        ):
        payload['record_id'] = record_id
    elif endpoint in ('match_affirm', 'match_deny'):
        payload['record_id_low'] = record_id_low
        payload['record_id_high'] = record_id_high
    with Auditor(user, version, endpoint) as job_auditor:
        response = COUPLER[endpoint]['processor'](payload, job_auditor)
    click.echo(f'{response}')


@cli.command('get')
@click.argument('endpoint')
@click.option('--transaction_key', default=None, 
    help='targeted transaction key')
@click.option('--state', default=None, 
    help='targeted state')
@click.option('--record_id_low', default=None, 
    help='targeted demographic record')
@click.option('--record_id_high', default=None, 
    help='targeted demographic record')
@click.option('--record_id', default=None, 
    help='targeted demographic record')
@click.option('--proc_id', default=None, 
    help='targeted process key')
@click.option('--postal_code', default=None, 
    help='targeted postal code')
@click.option('--name_day', default=None, 
    help='targeted name day')
@click.option('--middle_name', default=None, 
    help='targeted middle name')
@click.option('--given_name', default=None, 
    help='targeted given name')
@click.option('--gender', default=None, 
    help='targeted gender')
@click.option('--family_name', default=None, 
    help='targeted family name')
@click.option('--etl_id', default=None, 
    help='targeted primary key')
@click.option('--city', default=None, 
    help='targeted city')
@click.option('--batch_id', default=None, 
    help='targeted batch key')
@click.option('--action', default=None, 
    help='a named behavior')
def empi_get(
    endpoint, 
    action, 
    batch_id, 
    city, 
    etl_id, 
    family_name, 
    gender, 
    given_name, 
    middle_name, 
    name_day,
    postal_code, 
    proc_id, 
    record_id, 
    record_id_high, 
    record_id_low, 
    state, 
    transaction_key
    ):
    payload = dict()
    payload['action'] = action
    payload['batch_id'] = batch_id
    payload['city'] = city
    payload['etl_id'] = etl_id
    payload['family_name'] = family_name
    payload['gender'] = gender
    payload['given_name'] = given_name
    payload['middle_name'] = middle_name
    payload['postal_code'] = postal_code
    payload['proc_id'] = proc_id
    payload['record_id'] = record_id
    payload['record_id_high'] = record_id_high
    payload['record_id_low'] = record_id_low
    payload['state'] = state
    payload['transaction_key'] = transaction_key
    for k, v in payload.items():
        if v is None:
            del payload[k]
    response = COUPLER['query_records']['processor'](payload, endpoint)
    for row in response:
        click.echo(f'{row}')


if __name__ == "__main__":
    cli()
