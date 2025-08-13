# Infrastructure Templates

This directory contains infrastructure configuration templates for each supported cloud platform. These templates document what resources will be created during setup and provide cost/time estimates.

## Template Files

- **gcp-setup.yaml**: Google Cloud Platform infrastructure
- **aws-setup.yaml**: Amazon Web Services infrastructure  
- **azure-setup.yaml**: Microsoft Azure infrastructure

## Template Variables

Templates use environment variable placeholders that are substituted during deployment:

### Common Variables
- `${DEPLOYMENT_ID}`: Unique identifier for the deployment
- `${DEPLOY_PLATFORM}`: Target platform (gcp/aws/azure)
- `${DEPLOY_ENVIRONMENT}`: Environment name (dev/staging/prod)

### Platform-Specific Variables
- `${DEPLOY_GCP_PROJECT}`, `${DEPLOY_GCP_REGION}`, `${DEPLOY_COMPOSER_ENVIRONMENT}`
- `${DEPLOY_AWS_REGION}`, `${DEPLOY_MWAA_ENVIRONMENT}`, `${ACCOUNT_ID}`
- `${DEPLOY_AZURE_SUBSCRIPTION_ID}`, `${DEPLOY_AZURE_RESOURCE_GROUP}`, etc.

### Storage Variables
- `${DEPLOY_DAG_BUCKET}`: Bucket/container for DAG files
- `${SKYDAG_SOURCE_BUCKET}`: Source data bucket/container
- `${SKYDAG_DEST_BUCKET}`: Destination data bucket/container

## Using Templates

Templates are informational and used by the infrastructure modules during setup. They help you understand:

1. **What resources will be created**
2. **Estimated deployment time**
3. **Approximate costs**
4. **Configuration details**

## Cost Considerations

All cost estimates are approximate and based on standard pricing. Actual costs depend on:

- **Usage patterns**: How often DAGs run, data volume processed
- **Regional pricing**: Costs vary by cloud region
- **Resource utilization**: Actual compute and storage usage
- **Additional services**: Monitoring, logging, networking costs

## Time Estimates

Deployment times are estimates for clean environments:

- **GCP**: 20-45 minutes (mostly Composer environment creation)
- **AWS**: 20-30 minutes (mostly MWAA environment creation)  
- **Azure**: 5-10 minutes (Data Factory + Storage only)

*Note: Azure template creates storage and Data Factory only. A separate Airflow deployment would be needed.*

## Production Considerations

These templates create basic infrastructure suitable for demos and development. For production:

1. **Security**: Implement proper IAM roles, VPC/networking, encryption
2. **Monitoring**: Add CloudWatch/Stackdriver/Azure Monitor integration
3. **Backup**: Implement backup strategies for data and configurations
4. **Scaling**: Consider auto-scaling, load balancing, high availability
5. **Cost optimization**: Use reserved instances, lifecycle policies, monitoring