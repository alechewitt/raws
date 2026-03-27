# Validation Overview

**Date:** 2026-03-27
**Services tested:** 29 (Tier 1 + Tier 2)
**Commands cataloged:** 2,840
**Unique issues filed:** 40 (across issues-02.json through issues-05.json)

## Results by Service

| Service | Tested | Passed | Failed | Notes |
|---------|--------|--------|--------|-------|
| global_options | 12 | 8 | 4 | --version missing, help/table/error format |
| authentication | 4 | 2 | 2 | Error format differs |
| sts | 12 | 3 | 9 | Core works, error format differs |
| iam | 179 | 30 | 149 | URL-encoded policies, signing bug with optional params |
| s3api | 109 | 10 | 99 | Null fields, redirect handling, head-bucket broken |
| ec2 | 756 | 30 | 9 | --owners filter ignored (critical) |
| dynamodb | 59 | 4 | 54 | Systemic only |
| lambda | 86 | 6 | 80 | Systemic only |
| kms | 54 | 7 | 47 | Systemic only |
| s3 | 10 | 1 | 9 | Object listing broken (path-style URLs) |
| sns | 43 | 8 | 35 | Systemic only |
| sqs | 24 | 1 | 23 | **Params not serialized into JSON body** |
| cloudformation | 94 | 31 | 63 | Systemic only |
| ecs | 68 | 11 | 29 | Custom commands missing (deploy) |
| rds | 167 | 31 | 39 | **--output text/table broken** |
| cloudwatch | 45 | 2 | 43 | **Blocked: unsupported smithy-rpc-v2-cbor** |
| logs | 106 | 15 | 91 | **Params not serialized into JSON body** |
| secretsmanager | 24 | 2 | 22 | **--region ignored with service params** |
| ssm | 148 | 37 | 1 | **--output/--query ignored** |
| route53 | 73 | 7 | 66 | Signing issue on get-account-limit |
| cloudfront | 170 | 15 | 16 | Empty list suppression difference |
| elasticache | 77 | 34 | 2 | list-tags-for-resource auth error |
| elb | 31 | 4 | 27 | Query param serialization broken |
| elbv2 | 53 | 8 | 41 | Systemic only |
| **autoscaling** | **67** | **67** | **0** | **100% pass** |
| apigateway | 125 | 0 | 15 | Empty lists as null, features string vs array |
| kinesis | 40 | 10 | 30 | **--region priority bug**, ARN serialization |
| stepfunctions | 38 | 4 | 34 | **Params not serialized into JSON body** |
| codebuild | 60 | 13 | 30 | Systemic only |
| codepipeline | 45 | 5 | 40 | Systemic only |
| ecr | 61 | 0 | 60 | **Wrong SigV4 signing service name** |

## Top Priority Issues

### Critical (would unblock the most commands)

1. **JSON protocol param serialization** -- broken for SQS, Logs, Step Functions (not DynamoDB/KMS/ECS). Parameters are sent as null in the request body despite being provided on the command line. Only parameterless commands work. (issues-03, issues-04, issues-05)

2. **ECR signing service name** -- raws uses the wrong service name in SigV4 credential scope. AWS expects `ecr` but raws uses something else (possibly `api.ecr`). All 57 ECR API operations are completely blocked. (issues-05)

3. **--region flag priority** -- `AWS_REGION` env var overrides the `--region` CLI flag. AWS CLI correctly prioritizes `--region` over `AWS_REGION`. Confirmed on Kinesis, likely affects all services. (issues-05)

4. **IAM optional params signing** -- Adding any optional parameter to IAM commands (e.g., `list-policies --scope Local`) causes `SignatureDoesNotMatch: Credential should be scoped to a valid region`. Works fine without optional params. Likely a query protocol signing bug on the IAM global endpoint. (issues-02)

5. **S3 redirect/path-style handling** -- S3 operations on cross-region buckets return `PermanentRedirect` instead of transparently following the redirect. `s3 ls s3://<bucket>/` fails with HTTP 301. raws may be using path-style URLs instead of virtual-hosted-style. (issues-02)

6. **EC2 --owners/--owner-ids filter** -- `describe-images --owners self` and `describe-snapshots --owner-ids self` ignore the filter parameter, returning ALL public images/snapshots (79K+ items) instead of self-owned only. EC2 protocol list parameter serialization (`Owner.N`) is broken. (issues-02)

7. **CloudWatch smithy-rpc-v2-cbor** -- CloudWatch uses a protocol raws does not support. All 43 API commands fail. Adding this protocol would unblock the entire service. (issues-04)

### Medium (output parity)

8. **Null field inclusion** -- raws includes null-valued fields in JSON output; AWS CLI omits them. Affects s3api, dynamodb, lambda, route53, cloudfront, codebuild, and likely all services. (issues-02)

9. **Missing client-side parameter validation** -- raws sends requests to the server without validating required params, getting server-side errors (exit 254/255). AWS CLI validates client-side with `the following arguments are required: --param-name` (exit 252). This is the single largest source of test failures across all services. (issues-02)

10. **--output text/table broken for some services** -- RDS, SSM, and Kinesis commands ignore `--output text` and `--output table` flags, always returning JSON. Other services (STS, EC2, IAM) handle output formats correctly. (issues-04)

11. **Error message prefix missing** -- raws error messages lack the `aws: [ERROR]: ` prefix that AWS CLI prepends to all error output. (issues-02)

12. **Timestamps UTC vs local timezone** -- raws outputs timestamps in UTC (+00:00) while AWS CLI uses local timezone. Affects secretsmanager, ssm, kinesis, and any service with timestamp fields. (issues-04)

13. **Table output formatting** -- raws renders nested objects as inline JSON strings instead of sub-tables. Missing operation-name header row. Empty results produce no output instead of empty table frame. (issues-02)

### Additional Service-Specific Issues

14. **IAM URL-encoded policy documents** -- `list-roles` and `get-account-authorization-details` return AssumeRolePolicyDocument as raw URL-encoded strings instead of decoded JSON objects. (issues-02)

15. **Secrets Manager --region ignored with service params** -- Commands route to us-west-2 default instead of specified region when `--secret-id` or `--name` is present. (issues-04)

16. **RDS/SSM --query flag ignored** -- Returns full JSON instead of JMESPath-filtered results. (issues-04)

17. **API Gateway empty lists as null** -- `get-rest-apis` etc. return `"items": null` instead of `"items": []`. `get-account` returns features as string instead of array. (issues-05)

18. **ELB query param serialization** -- `describe-load-balancer-attributes --load-balancer-name` doesn't send the parameter. (issues-05)
