# Net-Accrual Simulator 🪙📈

Interactive Streamlit app that lets you pick a **date range**, **direction**  
(LONG or SHORT) and **collateral** (WETH / USDC) and instantly visualise the
daily net-accrual for your position.

---

## 1 · Quick start

```bash
git clone https://github.com/<your-handle>/net-accrual-simulator.git
cd net-accrual-simulator

# (recommended) isolate dependencies
python -m venv .venv           # or: conda create -n sim python=3.11
source .venv/bin/activate      # Windows: .\.venv\Scripts\activate

# install requirements
pip install -r requirements.txt

# drop your processed_data.csv next to simulator.py
python -m streamlit run simulator.py