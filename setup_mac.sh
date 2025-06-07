#!/bin/bash

# Colors for output
green="\033[0;32m"
red="\033[0;31m"
yellow="\033[0;33m"
blue="\033[0;34m"
cyan="\033[0;36m"
light_blue="\033[1;34m"
teal="\033[0;96m"
reset="\033[0m"

# ASCII art header with blue-green gradient
header="
${light_blue}
 █████╗  ██████╗ ███████╗███╗   ██╗████████╗██╗ ██████╗    ██████╗  ██████╗  ██████╗
${blue}██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝██║██╔════╝    ██╔══██╗██╔═══██╗██╔════╝
${cyan}███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   ██║██║         ██║  ██║██║   ██║██║     
${teal}██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   ██║██║         ██║  ██║██║   ██║██║     
${green}██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   ██║╚██████╗    ██████╔╝╚██████╔╝╚██████╗
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝ ╚═════╝    ╚═════╝  ╚═════╝  ╚═════╝

${light_blue}
███╗   ███╗██╗███╗   ██╗██╗███╗   ██╗ ██████╗ 
${blue}████╗ ████║██║████╗  ██║██║████╗  ██║██╔════╝ 
${cyan}██╔████╔██║██║██╔██╗ ██║██║██╔██╗ ██║██║  ███╗
${teal}██║╚██╔╝██║██║██║╚██╗██║██║██║╚██╗██║██║   ██║
${green}██║ ╚═╝ ██║██║██║ ╚████║██║██║ ╚████║╚██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ 
${reset}

${cyan}Welcome to AGENTIC DOC MINING Setup${reset}
${teal}-----------------------------------${reset}

This script will guide you through setting up the agentic document processing pipeline.
It will install Python 3.12, set up Prefect, and configure the Landing AI agentic-doc package.

${red}Note: This script requires Homebrew to be installed on your system.${reset}
"

# Function to print section headers
print_header() {
    echo -e "\n${green}=== $1 ===${reset}"
}

# Function to print errors
print_error() {
    echo -e "\n${red}Error: $1${reset}"
    echo -e "${red}Setup failed. Please fix the error and try again.${reset}"
    exit 1
}

# Function to print success messages
print_success() {
    echo -e "\n${green}✓ $1${reset}"
}

# Function to print info messages
print_info() {
    echo -e "${yellow}• $1${reset}"
}

# Function to print status updates
print_status() {
    echo -e "${blue}→ $1${reset}"
}

# Function to wait for user input
wait_for_input() {
    read -p "Press Enter to continue..." 
}

# Function to check command exists
cmd_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to verify installation
verify_install() {
    if ! $1; then
        print_error "$2"
    else
        print_success "$3"
    fi
}

# Display header
echo -e "$header"
sleep 1

# Check Homebrew
echo -e "${blue}Checking system requirements...${reset}"
if ! cmd_exists brew; then
    print_error "Homebrew is not installed. Please install Homebrew first."
    echo -e "${yellow}You can install Homebrew by running:\n${reset}"
    echo -e "${green}    /bin/bash -c "\"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""${reset}"
    exit 1
fi
print_success "Homebrew is installed"

# Update Homebrew
print_status "Updating Homebrew"
if brew update; then
    print_success "Homebrew updated successfully"
else
    print_error "Failed to update Homebrew"
fi

# Install Python 3.12
print_status "Installing Python 3.12"
if brew install python@3.12; then
    python3.12 --version
    print_success "Python 3.12 installed successfully"
else
    print_error "Failed to install Python 3.12"
fi

# Set up Python 3.12 as default
print_status "Setting up Python 3.12"
export PATH="/opt/homebrew/opt/python@3.12/bin:$PATH"

# Verify Python installation
print_status "Verifying Python installation"
if python3.12 --version; then
    print_success "Python 3.12 is ready to use"
else
    print_error "Python 3.12 not found in PATH"
fi

# Create and activate virtual environment
print_status "Creating virtual environment"
if python3.12 -m venv venv; then
    source venv/bin/activate
    echo "Python path: $(which python)"
    print_success "Virtual environment activated"
else
    print_error "Failed to create virtual environment"
fi

# Install requirements
print_status "Installing dependencies"
if pip install -r requirements.txt; then
    echo "Installed packages:"
    pip list | head -10
    echo "... (showing first 10 packages)"
    print_success "Dependencies installed successfully"
else
    print_error "Failed to install dependencies"
fi

# Configure Prefect to use local server
print_status "Configuring Prefect for local use"
prefect profile create local 2>/dev/null || true
prefect profile use local
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# Start Prefect server
print_status "Starting Prefect server"
prefect server start &
sleep 10  # Give Prefect more time to start
print_success "Prefect server started"

# Configure credentials and create blocks
print_header "Configuring credentials and creating Prefect blocks"

# Check if environment variables are set
print_status "Checking for environment variables"

# Debug: Show which env vars are set
[[ -n "$AWS_ACCESS_KEY_ID" ]] && echo -e "${green}✓ AWS_ACCESS_KEY_ID found${reset}" || echo -e "${red}✗ AWS_ACCESS_KEY_ID missing${reset}"
[[ -n "$AWS_SECRET_ACCESS_KEY" ]] && echo -e "${green}✓ AWS_SECRET_ACCESS_KEY found${reset}" || echo -e "${red}✗ AWS_SECRET_ACCESS_KEY missing${reset}"
[[ -n "$SNOWFLAKE_USER" ]] && echo -e "${green}✓ SNOWFLAKE_USER found${reset}" || echo -e "${red}✗ SNOWFLAKE_USER missing${reset}"
[[ -n "$SNOWFLAKE_PRIVATE_KEY_PATH" ]] && echo -e "${green}✓ SNOWFLAKE_PRIVATE_KEY_PATH found${reset}" || echo -e "${red}✗ SNOWFLAKE_PRIVATE_KEY_PATH missing${reset}"
[[ -n "$SNOWFLAKE_ACCOUNT" ]] && echo -e "${green}✓ SNOWFLAKE_ACCOUNT found${reset}" || echo -e "${red}✗ SNOWFLAKE_ACCOUNT missing${reset}"
[[ -n "$LANDING_AI_API_KEY" ]] && echo -e "${green}✓ LANDING_AI_API_KEY found${reset}" || echo -e "${red}✗ LANDING_AI_API_KEY missing${reset}"
[[ -n "$S3_BUCKET_NAME" ]] && echo -e "${green}✓ S3_BUCKET_NAME found${reset}" || echo -e "${red}✗ S3_BUCKET_NAME missing${reset}"

if [[ -n "$AWS_ACCESS_KEY_ID" && -n "$AWS_SECRET_ACCESS_KEY" && -n "$SNOWFLAKE_USER" && -n "$SNOWFLAKE_PRIVATE_KEY_PATH" && -n "$SNOWFLAKE_ACCOUNT" && -n "$LANDING_AI_API_KEY" && -n "$S3_BUCKET_NAME" ]]; then
    print_success "Found all required credentials in environment variables - auto-configuring blocks"
    aws_access_key="$AWS_ACCESS_KEY_ID"
    aws_secret_key="$AWS_SECRET_ACCESS_KEY"
    snowflake_user="$SNOWFLAKE_USER"
    snowflake_private_key_path="$(echo "$SNOWFLAKE_PRIVATE_KEY_PATH" | tr -d '\n\r')"
    snowflake_account="$SNOWFLAKE_ACCOUNT"
    landing_ai_key="$LANDING_AI_API_KEY"
    s3_bucket_name="$S3_BUCKET_NAME"
    configure_blocks="y"
else
    echo -e "${yellow}Some environment variables missing. Do you want to configure credentials manually?${reset}"
    read -p "Enter 'y' to configure now, or 'n' to skip [y/n]: " configure_blocks
fi

if [[ "$configure_blocks" == "y" || "$configure_blocks" == "Y" ]]; then
    print_status "Using environment variables for credentials"
elif [[ "$configure_blocks" == "manual" ]]; then
    read -p "Enter AWS Access Key ID: " aws_access_key
    read -s -p "Enter AWS Secret Access Key: " aws_secret_key
    echo
    read -p "Enter Snowflake User: " snowflake_user
    read -p "Enter path to Snowflake Private Key file: " snowflake_private_key_path
    read -p "Enter Snowflake Account: " snowflake_account
    read -s -p "Enter Landing AI API Key: " landing_ai_key
    echo
    read -p "Enter S3 Bucket Name: " s3_bucket_name
    configure_blocks="y"
fi

if [[ "$configure_blocks" == "y" || "$configure_blocks" == "Y" ]]; then
    print_status "Creating all Prefect blocks"
    python flows/setup_blocks.py "$aws_access_key" "$aws_secret_key" "$snowflake_user" "$snowflake_private_key_path" "$snowflake_account" "$landing_ai_key" "$s3_bucket_name"

    if [ $? -eq 0 ]; then
        print_success "All Prefect blocks created successfully"
        blocks_created=true
    else
        print_error "Failed to create Prefect blocks"
        blocks_created=false
    fi
else
    print_info "Skipping credentials configuration. You can set up blocks later manually."
    blocks_created=false
fi

# Deploy and serve the flow only if blocks were created
if [ "$blocks_created" = true ]; then
    print_status "Creating and serving flow deployment"
    cd /Users/mitchzink/Documents/GitHub/agentic-document-extraction-s3-to-snowflake
    python flows/s3_to_snowflake.py &
    DEPLOY_PID=$!
    sleep 8
    if ps -p $DEPLOY_PID > /dev/null; then
        print_success "Flow deployment created and serving"
        print_info "Deployment PID: $DEPLOY_PID"
        deployment_successful=true
        
        # Run the deployment immediately
        print_status "Running the deployment now"
        prefect deployment run 'S3 to Snowflake Document Extraction/s3-to-snowflake-extraction'
        print_success "Flow run triggered successfully"
    else
        print_error "Flow deployment failed"
        deployment_successful=false
    fi
else
    print_info "Skipping flow deployment. Create blocks first with real credentials."
    deployment_successful=false
fi

# Final success message
echo -e "\n${green}Setup completed successfully!${reset}"
if [ "$deployment_successful" = true ]; then
    echo -e "${yellow}✓ Flow deployment serving with hourly schedule${reset}"
    echo -e "${yellow}✓ Initial flow run triggered${reset}"
    echo -e "${yellow}To trigger manual runs:${reset}"
    echo -e "${green}    prefect deployment run 'S3 to Snowflake Document Extraction/s3-to-snowflake-extraction'${reset}"
    echo -e "${yellow}To stop the deployment server:${reset}"
    echo -e "${green}    kill $DEPLOY_PID${reset}"
else
    echo -e "${yellow}Flow not deployed. To create deployment:${reset}"
    echo -e "${green}1. Create blocks with real credentials: ./setup_mac.sh${reset}"
    echo -e "${green}2. Then run: python flows/s3_to_snowflake.py${reset}"
fi
echo -e "${yellow}Monitor flows at: http://localhost:4200${reset}"

# Open Prefect UI in browser
print_status "Opening Prefect UI in browser"
sleep 2
open http://localhost:4200
