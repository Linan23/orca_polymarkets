# Complete Project Setup & Run Guide

This guide provides step-by-step instructions to run the entire Orca Polymarket project locally, including the database, backend APIs, and frontend.

## Project Architecture

The project consists of:
- **PostgreSQL Database** (Docker): Port 5433
- **App API** (FastAPI): Port 8000
- **Data Platform API** (FastAPI): Port 8001
- **React Frontend** (Vite): Port 5173

## Prerequisites

Ensure you have installed:
- Docker & Docker Compose
- Python 3.10+
- Node.js 22.12+
- Git

## Step-by-Step Setup Instructions

### Step 1: Clone the Repository
```bash
cd /home/lynchej/orca_polymarkets
```

### Step 2: Create Virtual Environment (Python)
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### Step 3: Install Python Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Install Node.js Dependencies
```bash
cd my-app
npm install
cd ..
```

### Step 5: Start Docker Compose Stack
This will start all services in detached mode:
```bash
docker compose -f app/compose.yaml up -d
```

This starts:
- PostgreSQL database (orcaDB)
- Data Platform API (port 8001)
- App API (port 8000)
- React Frontend (port 5173)

### Step 6: Initialize Database Schema
```bash
export DATABASE_URL="postgresql+psycopg://app:password@139.147.9.248:5433/app_db"
export PSQL_URL="postgresql://app:password@139.147.9.248:5433/app_db"

# Run Alembic migrations to create tables
.venv/bin/alembic -c alembic.ini upgrade head
```

### Step 7: Verify Services Are Running
```bash
# Check Docker containers
docker compose -f app/compose.yaml ps

# Test API endpoints
curl http://localhost:8000/          # App API
curl http://localhost:8001/          # Data Platform API
curl http://localhost:5173/          # React Frontend (should return HTML)
```

## Access the Application

Once everything is running:

- **Frontend**: http://localhost:5173
- **App API**: http://localhost:8000
- **Data Platform API**: http://localhost:8001
- **Database**: You can connect using the connection string above

## Useful Commands

### View Logs
```bash
# All services
docker compose -f app/compose.yaml logs -f

# Specific service
docker compose -f app/compose.yaml logs -f data_platform_api
docker compose -f app/compose.yaml logs -f react_app
docker compose -f app/compose.yaml logs -f db
```

### Connect to Database
```bash
./data_platform/open_psql.sh
```

### Stop All Services
```bash
docker compose -f app/compose.yaml down
```

### Stop All Services and Remove Data
```bash
docker compose -f app/compose.yaml down -v
```

### Rebuild Docker Images
```bash
docker compose -f app/compose.yaml down
docker compose -f app/compose.yaml build --no-cache
docker compose -f app/compose.yaml up -d
```

### Build Frontend
```bash
cd my-app
npm run build
cd ..
```

## Environment Variables

Key environment variables used by the services:

**Database:**
- `DATABASE_URL`: postgresql+psycopg://app:password@localhost:5433/app_db
- `PSQL_URL`: postgresql://app:password@localhost:5433/app_db

**API:**
- `PYTHONPATH`: /workspace
- `FRONTEND_ORIGIN`: http://localhost:5173

**Frontend:**
- `VITE_API_BASE_URL`: http://localhost:8001

## Troubleshooting

### Port Already in Use
If ports 5433, 8000, 8001, or 5173 are already in use:
```bash
# Find and kill the process using the port
lsof -i :5433  # Check port 5433
kill -9 <PID>
```

### Database Connection Errors
```bash
# Wait for database to be ready
docker compose -f app/compose.yaml logs db

# Reset database
docker compose -f app/compose.yaml down -v
docker compose -f app/compose.yaml up -d db
sleep 10
.venv/bin/alembic -c alembic.ini upgrade head
```

### Frontend Not Loading
```bash
# Check Node modules
cd my-app
rm -rf node_modules package-lock.json
npm install
cd ..

# Restart the service
docker compose -f app/compose.yaml restart react_app
```

### API Connection Issues
```bash
# Check if APIs are running
docker compose -f app/compose.yaml logs data_platform_api
docker compose -f app/compose.yaml logs app_api

# Restart the services
docker compose -f app/compose.yaml restart data_platform_api app_api
```

## Next Steps

After everything is running:
1. Access your frontend at http://localhost:5173
2. Check API documentation at http://localhost:8001/docs (FastAPI auto-docs)
3. Query the database with `.venv/bin/sqlalchemy` or psql
4. Review data in dashboard snapshot (if available)

## Quick Start (One Command)

For faster setup after prerequisites are installed:
```bash
source .venv/bin/activate && \
pip install -r requirements.txt && \
cd my-app && npm install && cd .. && \
docker compose -f app/compose.yaml up -d && \
sleep 15 && \
export DATABASE_URL="postgresql+psycopg://app:password@localhost:5433/app_db" && \
.venv/bin/alembic -c alembic.ini upgrade head && \
echo "Setup complete! Services running on ports 5173 (frontend), 8000 (app), 8001 (api), 5433 (db)"
```
