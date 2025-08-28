#!/usr/bin/env python3

# Kubernetes Catalog Population Script
# This script discovers Kubernetes resources, creates Harness IDP entities for them, 
# and registers them in Harness catalog with GitHub integration

import os
import re
import json
import base64
import requests
import argparse
from dotenv import load_dotenv
from kubernetes import client, config

# Load environment variables
load_dotenv()

# === CONFIGURATION ===
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_ORG = os.getenv("GITHUB_ORG")
CENTRAL_REPO = os.getenv("CENTRAL_REPO")
HARNESS_API_KEY = os.getenv("HARNESS_API_KEY")
HARNESS_ACCOUNT_ID = os.getenv("HARNESS_ACCOUNT_ID")
CONNECTOR_REF = os.getenv("CONNECTOR_REF")
ORG_IDENTIFIER = os.getenv("ORG_IDENTIFIER")
PROJECT_IDENTIFIER = os.getenv("PROJECT_IDENTIFIER")

# === HEADERS ===
GITHUB_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

HARNESS_HEADERS = {
    "Content-Type": "application/json",
    "Harness-Account": HARNESS_ACCOUNT_ID,
    "x-api-key": HARNESS_API_KEY
}

# === HELPER FUNCTIONS ===
def generate_harness_identifier(resource_name, resource_namespace, resource_kind):
    """Generate a unique Harness identifier based on resource metadata"""
    # Combine name, namespace and kind for uniqueness
    name = f"{resource_namespace}_{resource_kind}_{resource_name}".lower()
    
    # Replace invalid characters with underscore
    name = re.sub(r"[^a-z0-9_$]", "_", name)
    
    # Ensure it starts with a letter or underscore
    if not re.match(r"^[a-z_]", name):
        name = f"k8s_{name}"
    
    # Removed random suffix to ensure stable, deterministic identifiers
    # Truncate to 128 characters max (as per Harness spec)
    return name[:128]

# === KUBERNETES FUNCTIONS ===
def get_kubernetes_resources(namespace=None, resource_kind=None):
    """
    Get Kubernetes resources filtered by namespace and kind
    """
    print(f"Discovering Kubernetes resources...")
    
    try:
        # Load Kubernetes configuration
        config.load_kube_config()
        print("Successfully connected to Kubernetes cluster")
        
        # Initialize API client
        v1 = client.CoreV1Api()
        apps_v1 = client.AppsV1Api()
        
        # Store all services for dependency mapping
        all_services = []
        
        resources = []
        
        # Define which resources to fetch based on the kind filter
        resource_types = []
        if resource_kind:
            resource_kind = resource_kind.lower()
            if resource_kind == "deployment":
                resource_types = ["deployment"]
            elif resource_kind == "service":
                resource_types = ["service"]
            elif resource_kind == "pod":
                resource_types = ["pod"]
            else:
                # If specific kind requested but not handled above, just try that kind
                resource_types = [resource_kind.lower()]
        else:
            # Default: fetch only stable resources (excluding Pods as they are ephemeral)
            resource_types = ["deployment", "service"]
        
        # Fetch resources
        for resource_type in resource_types:
            try:
                if resource_type == "deployment":
                    if namespace:
                        items = apps_v1.list_namespaced_deployment(namespace=namespace).items
                    else:
                        items = apps_v1.list_deployment_for_all_namespaces().items
                    
                    for item in items:
                        # Extract environment variables for dependency detection
                        env_vars = {}
                        containers = item.spec.template.spec.containers
                        for container in containers:
                            if container.env:
                                for env in container.env:
                                    if env.value:
                                        env_vars[env.name] = env.value
                        
                        resources.append({
                            "name": item.metadata.name,
                            "namespace": item.metadata.namespace,
                            "kind": "Deployment",
                            "labels": item.metadata.labels if item.metadata.labels else {},
                            "env_vars": env_vars,
                            "selector": item.spec.selector.match_labels if item.spec.selector and item.spec.selector.match_labels else {}
                        })
                        
                elif resource_type == "service":
                    if namespace:
                        items = v1.list_namespaced_service(namespace=namespace).items
                    else:
                        items = v1.list_service_for_all_namespaces().items
                    
                    for item in items:
                        service = {
                            "name": item.metadata.name,
                            "namespace": item.metadata.namespace,
                            "kind": "Service",
                            "labels": item.metadata.labels if item.metadata.labels else {},
                            "selector": item.spec.selector if item.spec.selector else {}
                        }
                        resources.append(service)
                        all_services.append(service)
                        
                elif resource_type == "pod":
                    if namespace:
                        items = v1.list_namespaced_pod(namespace=namespace).items
                    else:
                        items = v1.list_pod_for_all_namespaces().items
                    
                    for item in items:
                        resources.append({
                            "name": item.metadata.name,
                            "namespace": item.metadata.namespace,
                            "kind": "Pod",
                            "labels": item.metadata.labels or {},
                        })
            except Exception as resource_error:
                print(f"Error fetching {resource_type} resources: {str(resource_error)}")
        
        # Build service-to-deployment mapping
        service_to_deployment_map = {}
        
        for service in all_services:
            service_name = service["name"]
            service_selector = service["selector"]
            implementing_deployments = []
            
            # Find deployments that implement this service
            for resource in resources:
                if resource["kind"] == "Deployment":
                    deployment_labels = resource["labels"]
                    # Check if deployment labels match service selector
                    if service_selector and all(deployment_labels.get(k) == v for k, v in service_selector.items() if k in deployment_labels):
                        implementing_deployments.append(resource["name"])
            
            if implementing_deployments:
                service_to_deployment_map[service_name] = implementing_deployments
        
        # Store the service-to-deployment mapping with resources for dependency resolution
        for resource in resources:
            resource["service_to_deployment_map"] = service_to_deployment_map
        
        print(f"Found {len(resources)} Kubernetes resources")
        return resources
    
    except Exception as e:
        print(f"Error connecting to Kubernetes cluster: {str(e)}")
        return []

# === HARNESS CATALOG FUNCTIONS ===
def create_harness_entity(resource, all_resources=None):
    """
    Create Harness IDP entity for a Kubernetes resource
    """
    # Skip registration of Pod resources
    if resource['kind'] == "Pod":
        print(f"Skipping Pod {resource['name']} as Pods should not be registered as Components")
        return False
    print(f"Creating entity for: {resource['name']} ({resource['kind']} in {resource['namespace']})")
    
    # Generate a unique identifier for this resource
    identifier = generate_harness_identifier(resource['name'], resource['namespace'], resource['kind'])
    
    # Find dependencies based on environment variables
    dependencies = []
    if all_resources and resource['kind'] == "Deployment" and "env_vars" in resource:
        for env_name, env_value in resource["env_vars"].items():
            # Check for service references in environment variables
            for service in [r for r in all_resources if r["kind"] == "Service"]:
                service_name = service["name"]
                # Look for service name in environment variables
                if service_name.lower() in env_value.lower():
                    # Generate the service identifier
                    service_id = generate_harness_identifier(service_name, service["namespace"], service["kind"])
                    dependencies.append({
                        "name": service_name,
                        "identifier": service_id,
                        "type": "Service"
                    })
                    print(f"Detected dependency: {resource['name']} depends on service {service_name}")
                    
                    # If service is implemented by deployment(s), add those as indirect dependencies
                    if "service_to_deployment_map" in resource and service_name in resource["service_to_deployment_map"]:
                        for dep_name in resource["service_to_deployment_map"][service_name]:
                            # Skip self-references
                            if dep_name != resource["name"]:
                                # Find the deployment resource
                                dep_resource = next((r for r in all_resources if r["kind"] == "Deployment" and r["name"] == dep_name), None)
                                if dep_resource:
                                    dep_id = generate_harness_identifier(dep_name, dep_resource["namespace"], dep_resource["kind"])
                                    # Add as indirect dependency through service
                                    dependencies.append({
                                        "name": dep_name,
                                        "identifier": dep_id,
                                        "type": "Deployment",
                                        "via": service_name
                                    })
                                    print(f"Detected indirect dependency: {resource['name']} depends on deployment {dep_name} via {service_name}")
    
    # Create YAML content in Harness IDP format
    idp_yaml = f"""apiVersion: harness.io/v1
kind: component
orgIdentifier: {ORG_IDENTIFIER}
projectIdentifier: {PROJECT_IDENTIFIER}
type: Service
identifier: {identifier}
name: "{resource['name']}"
owner: group:account/IDP_Test
spec:
  lifecycle: production
  type: kubernetes
  subtype: {resource['kind']}
metadata:
  description: "Kubernetes {resource['kind']} {resource['name']} in namespace {resource['namespace']}"
  tags:
    - kubernetes
    - auto-onboarded
    - {resource['namespace']}
    - {resource['kind'].lower()}
"""

    # Rebuild YAML with proper dependency structure to avoid format issues
    # First parse the existing YAML to extract needed components
    yaml_parts = idp_yaml.split('spec:')
    header_part = yaml_parts[0]
    spec_part = yaml_parts[1].split('metadata:')[0]
    metadata_part = 'metadata:' + yaml_parts[1].split('metadata:')[1]
    
    # Rebuild the YAML with proper dependency format
    idp_yaml = header_part + 'spec:'
    
    # Add dependencies if any were found
    if dependencies:
        # Add dependsOn section as a proper array
        idp_yaml += '\n  dependsOn:'
        for dep in dependencies:
            if dep["type"] == "Service":
                # Direct dependency on a service
                idp_yaml += f'\n    - Component:{dep["name"]}'
            elif "via" in dep:
                # This is an indirect dependency through a service
                # Include comment to explain the indirect relationship
                idp_yaml += f'\n    # via service {dep["via"]}\n    - Component:{dep["name"]}'
    
    # Add the rest of the spec and metadata sections
    idp_yaml += '\n  lifecycle: production\n  type: kubernetes\n  subtype: ' + resource['kind'] + '\n' + metadata_part
    
    # File path in GitHub
    file_path = f"{resource['namespace']}/{resource['kind'].lower()}/{resource['name']}/idp.yaml"
    
    # Commit message
    commit_message = f"Add {resource['kind']} {resource['name']} from namespace {resource['namespace']}"
    
    # Let Harness handle the GitHub operation
    register_in_harness(idp_yaml, file_path, commit_message)
    return True

def push_to_github(file_path, content, commit_message):
    """
    Push file to GitHub repository
    """
    # GitHub API URL for creating or updating a file
    url = f"https://api.github.com/repos/{GITHUB_ORG}/{CENTRAL_REPO}/contents/{file_path}"
    
    try:
        # Check if file exists and get its SHA
        file_sha = None
        try:
            check_response = requests.get(url, headers=GITHUB_HEADERS)
            if check_response.status_code == 200:
                file_sha = check_response.json()["sha"]
        except:
            # Continue if we can't get the file (it probably doesn't exist)
            pass
        
        # Prepare content for upload
        encoded_content = base64.b64encode(content.encode()).decode()
        
        # Prepare payload
        payload = {
            "message": commit_message,
            "content": encoded_content
        }
        
        # Add SHA if file exists (for updates)
        if file_sha:
            payload["sha"] = file_sha
            
        # Create or update file
        response = requests.put(url, headers=GITHUB_HEADERS, json=payload)
        
        if response.status_code in [200, 201]:
            action = "Updated" if file_sha else "Created"
            print(f"{action} {file_path} in GitHub")
            return True
        else:
            print(f"Failed to {'update' if file_sha else 'create'} {file_path} in GitHub: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Error pushing to GitHub: {str(e)}")
        return False

def register_in_harness(yaml_content, file_path, commit_message):
    """
    Register entity in Harness using the Entities API
    """
    # The Harness API endpoint for creating entities
    harness_url = (
        f"https://qa.harness.io/v1/entities"
        f"?convert=false&dry_run=false"
        f"&orgIdentifier={ORG_IDENTIFIER}&projectIdentifier={PROJECT_IDENTIFIER}"
    )
    
    # Payload for Harness API
    payload = {
        "yaml": yaml_content,
        "git_details": {
            "branch_name": "main",
            "file_path": file_path,
            "commit_message": commit_message,
            "connector_ref": CONNECTOR_REF,
            "store_type": "REMOTE",
            "repo_name": CENTRAL_REPO,
            "is_harness_code_repo": False
        }
    }
    
    try:
        # Send the request to the Harness API
        response = requests.post(harness_url, headers=HARNESS_HEADERS, data=json.dumps(payload))
        
        # Check the response status code
        if response.status_code >= 200 and response.status_code < 300:
            print(f"✓ Registered in Harness successfully")
            return True
        else:
            print(f"✗ Harness ERROR: {response.status_code}")
            print(f"Details: {response.text[:200]}")
            
            # If entity already exists, try with operationMode=UPSERT
            if response.status_code == 400 and ("already exists" in response.text.lower() or 
                                              "does not match" in response.text.lower() or 
                                              "already a file" in response.text.lower()):
                print("Trying with UPSERT mode...")
                
                # Try with UPSERT mode
                upsert_url = (
                    f"https://qa.harness.io/v1/entities"
                    f"?convert=false&dry_run=false&operationMode=UPSERT"
                    f"&orgIdentifier={ORG_IDENTIFIER}&projectIdentifier={PROJECT_IDENTIFIER}"
                )
                
                upsert_response = requests.post(upsert_url, headers=HARNESS_HEADERS, data=json.dumps(payload))
                
                if upsert_response.status_code >= 200 and upsert_response.status_code < 300:
                    print(f"✓ Updated entity in Harness successfully")
                    return True
                else:
                    print(f"✗ Failed to update entity: {upsert_response.status_code}")
                    return False
            
            return False
    except Exception as e:
        print(f" Error registering in Harness: {str(e)}")
        return False

# === MAIN FUNCTION ===
def main():
    """Main function for Kubernetes catalog population"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Kubernetes Catalog Population')
    parser.add_argument('--namespace', type=str, help='Filter by Kubernetes namespace')
    parser.add_argument('--resource-kind', type=str, help='Filter by Kubernetes resource kind')
    parser.add_argument('--analyze-dependencies', action='store_true', help='Analyze and include dependencies')
    args = parser.parse_args()
    
    print("=== Kubernetes Catalog Population ===")
    print(f"Namespace filter: {args.namespace or 'All namespaces'}")
    print(f"Resource kind filter: {args.resource_kind or 'All kinds'}")
    if args.analyze_dependencies:
        print("Dependency analysis: Enabled")
    
    # Get Kubernetes resources
    resources = get_kubernetes_resources(
        namespace=args.namespace,
        resource_kind=args.resource_kind
    )
    
    # If no resources found, warn the user
    if not resources:
        print("Warning: No Kubernetes resources found with the specified filters!")
        print("Try running without filters or with different filters.")
        return
    
    # Process each resource
    github_success_count = 0
    harness_success_count = 0
    
    for resource in resources:
        print(f"\nProcessing {resource['kind']} {resource['name']} in namespace {resource['namespace']}...")
        
        # Create entity in both GitHub and Harness
        success = create_harness_entity(resource, resources if args.analyze_dependencies else None)
        
        # Track success
        if success:
            github_success_count += 1
            harness_success_count += 1
    
    # Print summary
    print("\n=== Summary ===")
    print(f"Total resources processed: {len(resources)}")
    print(f"Successfully pushed to GitHub: {github_success_count}")
    print(f"Successfully registered in Harness: {harness_success_count}")

if __name__ == "__main__":
    main()
