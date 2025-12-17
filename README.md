# Colab-ready MF-Structure Final Project

## Files
- `phi_input.txt`: query specification (S, n, V, F, sigma, G)
- `codegen.py`: generates `qpe.py` from `phi_input.txt`
- `qpe.py`: generated executable (connects to PostgreSQL, scans `sales`)

## Colab usage
```bash
pip -q install -r requirements.txt
# create .env (edit values)
cat > .env << 'EOF'
USER=postgres
PASSWORD=your_password
DBNAME=your_db
HOST=localhost
PORT=5432
EOF

python codegen.py phi_input.txt qpe.py
python qpe.py
```
