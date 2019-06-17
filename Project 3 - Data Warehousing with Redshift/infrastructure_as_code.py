import configparser, os, boto3, json, time, sys

config = configparser.ConfigParser()
config.read('dwh.cfg')

def write_config(config, file='dwh.cfg'):
    '''Store the new config in the file'''
    with open(file,'w') as f:
        config.write(f)

def _clean(s):
    '''Clean a string from all quotes
    s(string): string to be cleaned
    Returns: cleaned string'''
    return s.replace('"','').replace("'","").strip()

def _aws_keys_present():
    '''Checks if aws keys are present.
    Returns: bool
    '''
    global config
    config.read('dwh.cfg')
    if _clean(config.get('AWS','key')) and _clean(config.get('AWS','secret')):
        return True
    return False

def _config_complete():
    '''Checks if the config is complete without the aws keys.
    Returns: bool
    '''
    global config
    config.read('dwh.cfg')
    required_keys=['host','db_name','db_user','db_password','db_port','db_role_arn']
    for k in required_keys:
        if not _clean(config['CLUSTER'][k]):
            return False
    return True

def _credentials(clean=False):
    '''Utility function.  See get_credentials or wipe_credentials for info
    '''
    global config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config.set('AWS','key', os.environ.get('AWS_KEY', config.get('AWS','key')))
    config.set('AWS','secret', os.environ.get('AWS_SECRET', config.get('AWS','secret')))
    
    if _aws_keys_present():
        pass
    elif _config_complete():
        pass
    else:
        if not _aws_keys_present():
            m = 'AWS CREDENTIALS MISSING'
            m += '\nEnsure that AWS_KEY and AWS_SECRET is set as environment variables. '
            m += '\nAlternatively, set "key" and "secret" under "[AWS]" section in dwh.cfg'
            raise Exception(m)
        else:
            m = 'CONFIG INCOMPLETE'
            m += '\nEnsure that ALL of the following keys are correctly populated in the dwh.cfg file under [CLUSTER]'
            m += '\n[host, db_name, db_user, db_password, db_port, db_role_arn]'
            raise Exception(m)
    
    if clean:
        config.set('AWS','key', '')
        config.set('AWS','secret', '')
        config.set('CLUSTER','host','')
        config.set('CLUSTER','db_role_arn','')
        
    write_config(config)
    
    return _aws_keys_present()
        
def wipe_credentials():
    '''Wipe the credentials from the config file'''
    _credentials(True)

def load_credentials():
    '''Load AWS_KEY and AWS_SECRET from environment.  Uses fields in config as fallback
    Store in the config file for future use.
    clean: remove the options from the config
    '''
    return _credentials()
    
def _get_aws_params(required_keys=None):
    '''Get settings from config file
    required_keys(list): list of desired keys, i.e. ['region_name', 'RoleName']
    Returns dictionary containing the aws named parameters (required_keys)
    '''
    load_credentials()
    params = {}
    params['region_name']                = _clean(config.get('AWS','region'))
    params['aws_access_key_id']          = _clean(config.get('AWS','key'))
    params['aws_secret_access_key']      = _clean(config.get('AWS','secret'))
    params['RoleName']                   = _clean(config.get('IAM_ROLE','iam_role_name'))
    params['ClusterType']                = _clean(config.get('CLUSTER','db_cluster_type'))
    params['NodeType']                   = _clean(config.get('CLUSTER','db_node_type'))
    params['NumberOfNodes']              = int(_clean(config.get('CLUSTER','db_num_nodes')))
    params['DBName']                     = _clean(config.get('CLUSTER','db_name'))
    params['ClusterIdentifier']          = _clean(config.get('CLUSTER','db_cluster_identifier'))
    params['MasterUsername']             = _clean(config.get('CLUSTER','db_user'))
    params['MasterUserPassword']         = _clean(config.get('CLUSTER','db_password'))
    params['IamRoles']                   =[_clean(config.get('CLUSTER','db_role_arn'))]
    params['GroupName']                  = _clean(config.get('CLUSTER','db_security_group'))
    if required_keys:
        params = dict((k, params[k]) for k in params if k in required_keys)
    return params
    
def get_client(name):
    '''Create client and attach policies as needed for project
    name(string): name of client, i.e. 's3', 'iam', 'redshift'
    Returns: boto3.client
    '''
    required_keys = ['region_name','aws_access_key_id','aws_secret_access_key']
    return boto3.client(name, **_get_aws_params(required_keys))

def get_resource(name):
    '''Create client and attach policies as needed for project
    name(string): name of resource, i.e. 's3', 'iam', 'redshift'
    Returns: boto3.resource
    '''
    required_keys = ['region_name','aws_access_key_id','aws_secret_access_key']
    return boto3.resource(name, **_get_aws_params(required_keys))

def create_role():
    '''Create AWS role with name specified in dwh.cfg
    Creates the db_role_arn entry in the config file for later use
    '''
    if not load_credentials():
        return
    print('creating IAM role',end='...', flush=True)
    params = _get_aws_params(['RoleName'])
    params['Description'] = "Allows Redshift clusters to call AWS services on your behalf."
    policy = {'Statement': [{'Action': 'sts:AssumeRole',
                             'Effect': 'Allow',
                             'Principal': {'Service': 'redshift.amazonaws.com'}}],
              'Version': '2012-10-17'}
    params['AssumeRolePolicyDocument'] = json.dumps(policy)
    iam = get_client('iam')
    try:
        response = iam.create_role(**params)
        if response['ResponseMetadata']['HTTPStatusCode']==200: print('Role created!')
        response = iam.attach_role_policy(**_get_aws_params(['RoleName']),
                                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        if response['ResponseMetadata']['HTTPStatusCode']==200: print("AmazonS3ReadOnlyAccess policy attached to role")
    except Exception as e:
        if 'EntityAlreadyExists' in str(e): pass
        else: raise e
    arn = iam.get_role(**_get_aws_params(['RoleName']))['Role']['Arn']
    load_credentials()
    config.set('CLUSTER','db_role_arn',arn)
    write_config(config)    
    print('done!')

def create_redshift_cluster():
    '''Create the redshift cluster
    Creates entries in the config file for host and db_role_arn
    '''
    if not load_credentials():
        return
    print('creating redshift cluster', end='...', flush=True)
    if not _clean(config.get('CLUSTER','db_role_arn')):
        create_role()
    required_keys = ['ClusterType', 'NodeType', 'NumberOfNodes', 'DBName', 'ClusterIdentifier', 
                     'MasterUsername', 'MasterUserPassword', 'IamRoles']
    redshift = get_client('redshift')
    ec2 = get_resource('ec2')
    try:
        sgid = ec2.create_security_group(**_get_aws_params(['GroupName']), Description='redshift security group').id
    except Exception as e:
        if 'InvalidGroup.Duplicate' in str(e):
            sgid = ec2.security_groups.filter(GroupNames=[_clean(config.get('CLUSTER','DB_SECURITY_GROUP'))])
            sgid = [x.id for x in sgid][0]
        else: raise e
    try:
        redshift.create_cluster(**_get_aws_params(required_keys), 
                                PubliclyAccessible=True, VpcSecurityGroupIds=[sgid])
        for par in required_keys[:4]:
            print(f'{par.ljust(25)}:',_get_aws_params([par])[par])
    except Exception as e:
        if 'ClusterAlreadyExists' in str(e):
            print('Cluster already exists')
        else: raise e
    
    while redshift.describe_clusters(**_get_aws_params(['ClusterIdentifier']))['Clusters'][0]['ClusterStatus']=='creating':
        print('.',end='',flush=True)
        time.sleep(10)
    
    clusterProps = redshift.describe_clusters(**_get_aws_params(['ClusterIdentifier']))['Clusters'][0]
    print(clusterProps['ClusterStatus'])
    if clusterProps['ClusterStatus']=='available':
        DB_ENDPOINT = clusterProps['Endpoint']['Address']
        DB_ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']
        load_credentials()
        config.set('CLUSTER','host', DB_ENDPOINT)
        config.set('CLUSTER','db_role_arn',DB_ROLE_ARN)
        write_config(config)
    
    # Open incoming TCP port to access the cluster endpoint
    vpc = ec2.Vpc(id=clusterProps['VpcId'])
    sg = [x for x in vpc.security_groups.filter(GroupIds=[sgid])][0]
    try:
        sg.authorize_ingress(
            GroupName=sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.get('CLUSTER','DB_PORT')),
            ToPort=int(config.get('CLUSTER','DB_PORT'))
        )
        print('Opening TCP port')
    except Exception as e:
        if 'InvalidPermission.Duplicate' in str(e):
            print('TCP port already open')
        else: raise e

def cleanup_resources():
    '''Clean up the redshift resource and the created role
    CAUTION! Do not run this unless you want to drop the resources!
    '''
    if not load_credentials():
        return
    redshift = get_client('redshift')
    try:
        redshift.delete_cluster(**_get_aws_params(['ClusterIdentifier']), SkipFinalClusterSnapshot=True)
        print('Deleting Cluster')
        clusterProps = redshift.describe_clusters(**_get_aws_params(['ClusterIdentifier']))['Clusters'][0]
        while clusterProps['ClusterStatus']=='deleting':
            print('.', end='', flush=True)
            time.sleep(10)
            try:
                clusterProps = redshift.describe_clusters(**_get_aws_params(['ClusterIdentifier']))['Clusters'][0]
            except Exception as e:
                if 'ClusterNotFound' in str(e):
                    print('\nCluster deleted!')
                    break
                else: raise e
    except Exception as e:
        if 'ClusterNotFound' in str(e): print('Cluster already deleted!')
        elif 'InvalidClusterState' in str(e): print('Delete request already sent')
        else: raise e

    print('Detaching Policies from role')
    iam = get_client('iam')
    try:
        for i in iam.list_attached_role_policies(**_get_aws_params(['RoleName']))['AttachedPolicies']:
            iam.detach_role_policy(**_get_aws_params(['RoleName']), PolicyArn=i['PolicyArn'])
    except Exception as e:
        if 'NoSuchEntity' in str(e):
            print('Policy already detached')
        else: raise e
    time.sleep(1)

    print('Deleting role')
    try:
        iam.delete_role(**_get_aws_params(['RoleName']))
        print('Role Deleted')
    except Exception as e:
        if 'NoSuchEntity' in str(e):
            print('Role already deleted')
        else: raise e
            
    print('Deleting security group')
    try:
        ec2 = get_client('ec2')
        ec2.delete_security_group(**_get_aws_params(['GroupName']))
    except Exception as e:
        if 'InvalidGroup.NotFound':
            print('Security group already deleted')
        else: raise e
    print('Cleaned up all resources')
    wipe_credentials()
    
if __name__=='__main__':
    try:
        option = sys.argv[1].lower().strip()
    except IndexError:
        print('Please indicate the action to take: create or clean')
        option = input('[create/clean]]: ').lower()
    if 'create' not in option and 'clean' not in option:
        print('Please select the action to take: create or clean')
        option = input('[create/clean]]: ').lower()
    if 'create' in option:
        create_redshift_cluster()
    elif 'clean' in option:
        print('Cleaning up the infrastructure')
        cleanup_resources()
    else:
        print('Option must be one of "create" or "clean". Exiting.')