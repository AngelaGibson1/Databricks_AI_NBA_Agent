from databricks.agents import deploy, list_deployments, enable_trace_reviews
import json
import os

def deploy_agent_to_environment(model_name, env="dev"):
    """
    Deploy an agent to the specified environment
    
    Args:
        model_name: The name of the agent model in Unity Catalog
        env: The target environment (dev, test, prod)
    """
    # Load environment-specific configuration
    config_path = f"/Workspace/Repos/databricks-agent-playbook/config/{env}/agent_config.yml"
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    # Get the latest version
    model_version = get_latest_model_version(model_name)
    
    # Deploy the agent
    deployment = deploy(
        model_name=model_name,
        model_version=model_version,
        scale_to_zero=config.get("scale_to_zero", False),
        environment_vars=config.get("environment_vars", {}),
        workload_size=config.get("workload_size", "SMALL"),
        endpoint_name=f"{model_name}-{env}",
        tags={"environment": env}
    )
    
    print(f"Deployed {model_name} version {model_version} to {env}")
    print(f"Endpoint: {deployment.endpoint_url}")
    
    # Enable trace reviews for monitoring
    enable_trace_reviews(model_name)
    
    return deployment

# Deploy the NBA Analysis agent to dev
deploy_agent_to_environment("catalog.schema.nba_analysis_agent", "dev")
```
