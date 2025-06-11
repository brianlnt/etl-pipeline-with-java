#!/bin/bash

# Simple AWS ECS Deployment Script for Interview Showcase
set -e

# Configuration
PROJECT_NAME="sports-etl-pipeline"
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "NOT_SET")

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_step() {
    echo -e "${GREEN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Build the application
build_app() {
    print_step "Building Spring Boot application..."
    mvn clean package -DskipTests
    print_info "✅ Application built successfully!"
}

# Step 2: Build and push Docker image
build_and_push() {
    print_step "Building and pushing Docker image to ECR..."
    
    if [ "$AWS_ACCOUNT_ID" = "NOT_SET" ]; then
        print_error "AWS CLI not configured. Run 'aws configure' first."
        exit 1
    fi
    
    # Create ECR repository if it doesn't exist
    aws ecr describe-repositories --repository-names $PROJECT_NAME --region $AWS_REGION 2>/dev/null || \
    aws ecr create-repository --repository-name $PROJECT_NAME --region $AWS_REGION
    
    # Get ECR login
    aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
    
    # Build and push
    docker build -t $PROJECT_NAME .
    docker tag $PROJECT_NAME:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT_NAME:latest
    docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT_NAME:latest
    
    print_info "✅ Docker image pushed to ECR!"
    echo "Image URI: $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT_NAME:latest"
}

# Step 3: Create ECS task definition
create_task_definition() {
    print_step "Creating ECS task definition..."
    
    cat > task-definition.json << EOF
{
  "family": "$PROJECT_NAME",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::$AWS_ACCOUNT_ID:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "$PROJECT_NAME",
      "image": "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT_NAME:latest",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "ETL_MODE",
          "value": "NORMAL"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/$PROJECT_NAME",
          "awslogs-region": "$AWS_REGION",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
EOF

    # Create CloudWatch log group
    aws logs create-log-group --log-group-name "/ecs/$PROJECT_NAME" --region $AWS_REGION 2>/dev/null || true
    
    # Register task definition
    aws ecs register-task-definition --cli-input-json file://task-definition.json --region $AWS_REGION
    
    print_info "✅ Task definition created!"
}

# Step 4: Manual setup instructions
manual_setup() {
    print_step "Next Steps - Manual Setup in AWS Console:"
    echo ""
    echo "🌐 Go to AWS ECS Console: https://console.aws.amazon.com/ecs/"
    echo ""
    echo "1️⃣  Create ECS Cluster:"
    echo "   - Name: $PROJECT_NAME-cluster"
    echo "   - Infrastructure: AWS Fargate (serverless)"
    echo ""
    echo "2️⃣  Create Service (Optional - for continuous running):"
    echo "   - Launch type: Fargate"
    echo "   - Task Definition: $PROJECT_NAME:1"
    echo "   - Desired tasks: 1"
    echo ""
    echo "3️⃣  Run Task Manually (For ETL execution):"
    echo "   - Cluster: $PROJECT_NAME-cluster"
    echo "   - Task Definition: $PROJECT_NAME:1"
    echo "   - Launch type: Fargate"
    echo "   - Add environment variable: ETL_MODE = SCHEDULED"
    echo ""
    echo "4️⃣  Schedule with EventBridge (Optional):"
    echo "   - Go to EventBridge Console"
    echo "   - Create rule with cron: cron(0 2 * * ? *)"
    echo "   - Target: ECS Task"
    echo ""
    print_info "📋 Task Definition ARN: arn:aws:ecs:$AWS_REGION:$AWS_ACCOUNT_ID:task-definition/$PROJECT_NAME:1"
}

# Step 5: Test the deployment
test_deployment() {
    print_step "Testing deployment..."
    
    echo "Available clusters:"
    aws ecs list-clusters --region $AWS_REGION --query 'clusterArns' --output table
    
    echo ""
    echo "To run the ETL pipeline manually:"
    echo "aws ecs run-task \\"
    echo "  --cluster $PROJECT_NAME-cluster \\"
    echo "  --task-definition $PROJECT_NAME \\"
    echo "  --launch-type FARGATE \\"
    echo "  --network-configuration 'awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}' \\"
    echo "  --overrides '{\"containerOverrides\":[{\"name\":\"$PROJECT_NAME\",\"environment\":[{\"name\":\"ETL_MODE\",\"value\":\"SCHEDULED\"}]}]}' \\"
    echo "  --region $AWS_REGION"
}

# Step 6: View logs
view_logs() {
    print_step "Viewing application logs..."
    aws logs tail "/ecs/$PROJECT_NAME" --follow --region $AWS_REGION
}

# Show help
show_help() {
    echo "Simple AWS ECS Deployment for Interview Showcase"
    echo ""
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  build     - Build Spring Boot application"
    echo "  push      - Build and push Docker image to ECR"
    echo "  task      - Create ECS task definition"
    echo "  setup     - Show manual setup instructions"
    echo "  test      - Test deployment"
    echo "  logs      - View application logs"
    echo "  all       - Run build, push, task, and show setup"
    echo "  help      - Show this help"
    echo ""
    echo "Quick start: $0 all"
}

# Main execution
case "${1:-help}" in
    "build")
        build_app
        ;;
    "push")
        build_app
        build_and_push
        ;;
    "task")
        create_task_definition
        ;;
    "setup")
        manual_setup
        ;;
    "test")
        test_deployment
        ;;
    "logs")
        view_logs
        ;;
    "all")
        build_app
        build_and_push
        create_task_definition
        manual_setup
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        print_error "Unknown option: $1"
        show_help
        exit 1
        ;;
esac 