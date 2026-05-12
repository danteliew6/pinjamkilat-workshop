# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 0 — Setup + synthetic data
# MAGIC
# MAGIC **Workshop:** PinjamKilat Credit Decisioning (Bank Demo Sejahtera)
# MAGIC
# MAGIC We're standing up the data foundation for the workshop. By the end of this notebook you'll have:
# MAGIC
# MAGIC - A Unity Catalog catalog `workshop` with three schemas (`bronze`, `silver`, `gold`)
# MAGIC - **`workshop.bronze.loan_application`** — 5,000 synthetic Indonesian digital-loan applications with messy Bahasa free-text fields and KTP OCR strings
# MAGIC - **`workshop.bronze.transaction`** — ~50,000 bank-transaction rows (12 months of history per applicant)
# MAGIC - **`workshop.bronze.call_note`** — ~9,000 call-center notes in mixed Bahasa/English
# MAGIC
# MAGIC **Runtime:** ~3 minutes on Free Edition serverless.
# MAGIC
# MAGIC > **Why synthetic?** Free Edition has no sample loan datasets. We generate everything deterministically (`seed=42`) so every attendee gets identical data and the demo answers are stable.

# COMMAND ----------

# MAGIC %md ## Configure
# MAGIC
# MAGIC If your workspace doesn't allow you to create a new catalog, change `CATALOG` to one you already own (e.g. `main` or your username).

# COMMAND ----------

CATALOG = "workshop"   # change if needed
SEED = 42
N_APPLICATIONS = 5_000

print(f"Target catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md ## Create catalog + schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'PinjamKilat workshop — synthetic Indonesian digital-loan data'")
spark.sql(f"USE CATALOG {CATALOG}")
for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").display()

# COMMAND ----------

# MAGIC %md ## Generate synthetic data
# MAGIC
# MAGIC We build everything with NumPy + pandas in the driver. Small enough to fit (<50MB) and avoids pip installs.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

rng = np.random.default_rng(SEED)

# --- Indonesian name pools ----------------------------------------------------
FIRST_M = ["Budi","Adi","Hadi","Joko","Bambang","Andi","Eko","Rudi","Surya","Hendra",
           "Bayu","Wahyu","Agus","Yusuf","Hasan","Indra","Rahmat","Slamet","Fajar","Rendra",
           "Reza","Iwan","Tono","Made","Wayan","Dimas","Bagus","Khairul","Tegar","Muhammad"]
FIRST_F = ["Sri","Ratna","Dewi","Siti","Nia","Putri","Maya","Lina","Rini","Tina",
           "Diah","Aisha","Mira","Kartika","Dian","Lia","Wati","Fitri","Nurul","Endang",
           "Ratih","Anita","Susi","Yuli","Marlina","Citra","Tania","Vera","Eka","Linda"]
LAST = ["Wijaya","Santoso","Pratama","Lestari","Sari","Kurniawan","Putra","Setiawan",
        "Hidayat","Susanto","Hartono","Wibowo","Nugroho","Saputra","Permata","Gunawan",
        "Halim","Kusuma","Widodo","Hakim","Suherman","Mulyana","Iskandar","Nasution",
        "Lubis","Simanjuntak","Tobing","Sukarno","Hartanto","Maulana"]

# --- Geography (skewed toward Tunaiku-like lending markets) -------------------
GEO = [
    ("DKI Jakarta","Jakarta Selatan","Tebet",6),
    ("DKI Jakarta","Jakarta Timur","Pulogadung",5),
    ("DKI Jakarta","Jakarta Barat","Kebon Jeruk",5),
    ("DKI Jakarta","Jakarta Utara","Kelapa Gading",4),
    ("Jawa Barat","Bekasi","Cikarang Utara",7),
    ("Jawa Barat","Bekasi","Cibitung",6),
    ("Jawa Barat","Karawang","Klari",7),
    ("Jawa Barat","Karawang","Telukjambe",5),
    ("Jawa Barat","Bandung","Bojongloa Kidul",6),
    ("Jawa Barat","Bandung","Cibeunying Kaler",5),
    ("Jawa Barat","Depok","Sukmajaya",4),
    ("Banten","Tangerang","Karawaci",6),
    ("Banten","Tangerang Selatan","Serpong",5),
    ("Jawa Timur","Surabaya","Wonokromo",6),
    ("Jawa Timur","Surabaya","Tegalsari",5),
    ("Jawa Timur","Sidoarjo","Waru",4),
    ("Jawa Tengah","Semarang","Tembalang",4),
    ("Sumatera Utara","Medan","Medan Kota",3),
]

# --- Occupation free-text templates (Bahasa) ---------------------------------
# Each template emits realistic, slightly-messy text the model will extract from.
OCCUPATIONS = [
    ("karyawan tetap", "stable", [
        "saya bekerja sebagai karyawan tetap di pabrik tekstil di {kab}, sudah {y} tahun. Gaji {g} juta per bulan, kadang lembur dapat bonus.",
        "Karyawan tetap PT {co} di {kab}, posisi staff produksi, masa kerja {y} tahun. Pendapatan bersih sekitar {g}jt.",
        "Saya {y} tahun di PT {co} sebagai operator mesin. Slip gaji setiap tanggal 25, take home {g} juta.",
    ]),
    ("PNS/ASN", "stable", [
        "PNS Pemda {kab} golongan III/b, sudah {y} thn. Gaji pokok plus tunjangan total kurang lebih {g} juta sebulan.",
        "ASN guru SD Negeri di {kab}, masa kerja {y} tahun. Pendapatan {g} juta.",
    ]),
    ("wirausaha", "variable", [
        "punya warung kelontong di {kab}, omzet harian variasi 500rb-1jt, untung bersih sekitar {g} juta per bulan",
        "wirausaha online shop fashion via Shopee dan Tiktok, pendapatan {g}jt per bulan tapi musiman, ramai pas gajian",
        "buka usaha kuliner ayam geprek di {kab}, sudah {y} tahun, profit {g}-{g2}jt per bulan",
    ]),
    ("freelance", "variable", [
        "freelance graphic design, kerja dari rumah, pendapatan bervariasi {g}-{g2}jt tergantung proyek",
        "fotografer wedding freelance area {kab}, peak season {g2}jt, low season {g}jt",
        "freelance content writer untuk beberapa media digital, fee per artikel, total sekitar {g}-{g2}jt/bln",
    ]),
    ("ojol", "informal", [
        "Driver Gojek dan Grab di {kab}, full time, pendapatan harian 200-400rb tergantung orderan, total kira-kira {g} juta sebulan.",
        "ojek online Grabbike di {kab}, sudah {y} tahun, pendapatan {g}jt per bulan kalau rajin narik.",
    ]),
    ("kontrak", "stable", [
        "Karyawan kontrak di PT {co}, kontrak diperpanjang setiap tahun, sudah {y} tahun. Gaji {g}jt + uang makan.",
        "Tenaga outsourcing di {co}, kerja di {kab}, kontrak per 12 bulan, take home {g} juta.",
    ]),
    ("buruh harian", "informal", [
        "Buruh proyek konstruksi di {kab}, harian 150-180rb, kerja tidak setiap hari, sebulan dapat sekitar {g}jt.",
        "Buruh pabrik harian lepas, area {kab}, pendapatan {g}jt tapi tidak setiap bulan dapat orderan.",
    ]),
]

COMPANIES = ["Sentosa Tekstil","Indofood","Astra Honda","Pertamina","Telkom","BCA","Mayora",
             "Unilever","Sinar Mas","Wings","KAO","Nestle Indonesia","Sido Muncul","Yamaha",
             "Suzuki Motor","Sampoerna","Garudafood","ABC Bumi","Kalbe Farma","Pegadaian"]

PURPOSE = [
    "Renovasi rumah di {kab}, ganti atap dan plafon, butuh dana cepat",
    "Modal usaha tambahan untuk warung, mau stock barang lebaran",
    "Biaya sekolah anak masuk SMP swasta tahun ajaran baru",
    "Konsolidasi utang kartu kredit, ada 2 kartu yang mau dilunasi",
    "Biaya pengobatan ibu di RS, BPJS tidak cover semua tindakan",
    "Beli motor untuk kerja ojek online, motor lama sudah tua",
    "Modal tambahan online shop, mau ikut campaign Harbolnas",
    "DP rumah subsidi BTN, baru dapat NUP, butuh cepat",
    "Biaya pernikahan, gedung sudah DP, kurang untuk catering",
    "Tambahan modal usaha, mau buka cabang ke-2 di {kab}",
]

# --- Call-center note templates ----------------------------------------------
CALL_GOOD = [
    "Suara ramah, kooperatif. Semua dokumen lengkap dan jelas. Pelanggan paham produk dengan baik.",
    "Verified phone OK. Customer answered all questions clearly. Income source consistent with KTP.",
    "Pelanggan menjawab semua pertanyaan dengan tenang, alamat domisili sesuai KTP, slip gaji match.",
    "Verifikasi by call sukses, customer cooperative, no red flags.",
    "Ringkas dan jelas, customer paham term & syarat, setuju dengan tenor dan bunga.",
    "Pengajuan dikonfirmasi, pelanggan menjawab dengan lugas, employment confirmed.",
]
CALL_BAD = [
    "Pelanggan suara tinggi saat ditanya pendapatan. Beberapa kali menutup telepon. Mengaku pendapatan tidak konsisten.",
    "Phone disconnected 3 times. When asked about employer address, customer changed story. Suspicious.",
    "Customer sangat agresif, refuses to share employer phone, story about employment doesn't match application.",
    "Beberapa pertanyaan dijawab ragu-ragu, alamat kerja berbeda dengan application form, perlu verifikasi tambahan.",
    "Background noise sangat ramai, suara tidak jelas, customer terburu-buru, application data tidak match dengan jawaban verbal.",
    "Pelanggan stress saat ditanya cicilan lain, mengaku ada 3 pinjaman online aktif. DBR concern.",
]
CALL_NEUTRAL = [
    "Verifikasi standard, customer kooperatif tapi data pekerjaan kurang lengkap, perlu follow-up dokumen.",
    "Phone OK, customer agreeable, but income source variable per their description, perlu klarifikasi.",
    "Standard verification, no major red flags, but employment tenure shorter than disclosed in form.",
]


def _name_and_gender(rng):
    if rng.random() < 0.5:
        return f"{rng.choice(FIRST_M)} {rng.choice(LAST)}", "L"
    return f"{rng.choice(FIRST_F)} {rng.choice(LAST)}", "P"


def _nik_for_geo(rng, provinsi, dob, gender):
    """Compose a 16-digit NIK that looks plausible (real NIKs encode province + DOB)."""
    prov_code = {"DKI Jakarta":"31","Jawa Barat":"32","Banten":"36","Jawa Timur":"35",
                 "Jawa Tengah":"33","Sumatera Utara":"12"}.get(provinsi,"32")
    kab_code = rng.integers(1,99)
    kec_code = rng.integers(1,30)
    day = dob.day + (40 if gender == "P" else 0)
    body = f"{prov_code}{kab_code:02d}{kec_code:02d}{day:02d}{dob.month:02d}{dob.year%100:02d}"
    return body + f"{rng.integers(1000,9999):04d}"


def _ktp_ocr(rng, nik, name, gender, dob, place_of_birth):
    """Render KTP as messy OCR-style text. We deliberately add noise so ai_extract has work to do."""
    fmt = rng.integers(0, 3)
    if fmt == 0:
        # Spaces in NIK
        nik_disp = " ".join([nik[i:i+4] for i in range(0, 16, 4)])
        return f"NIK : {nik_disp} // {name.upper()} {gender} {dob.strftime('%d-%m-%Y')} {place_of_birth.upper()}"
    elif fmt == 1:
        return f"NIK:{nik}\n{name.upper()}\n{gender} {dob.strftime('%d/%m/%y')}\nTempat lahir: {place_of_birth}"
    else:
        # Slightly garbled
        return f"PROVINSI    KAB {place_of_birth.upper()}    NIK :{nik} \n Nama : {name}  Jenis Kelamin: {gender}  TTL: {place_of_birth}, {dob.strftime('%d %b %Y')}"


def _occupation(rng, kabupaten):
    cat, stability, templates = OCCUPATIONS[rng.integers(0, len(OCCUPATIONS))]
    tmpl = templates[rng.integers(0, len(templates))]
    income = int(rng.uniform(3, 18))
    return cat, stability, tmpl.format(
        kab=kabupaten,
        co=rng.choice(COMPANIES),
        y=rng.integers(1, 12),
        g=income,
        g2=income + rng.integers(2, 8),
    )


def _purpose_text(rng, kabupaten):
    return rng.choice(PURPOSE).format(kab=kabupaten)


# --- Build loan_application ---------------------------------------------------
print(f"Generating {N_APPLICATIONS:,} loan applications...")

today = datetime(2026, 5, 11)
gw = np.array([g[3] for g in GEO], dtype=float)
gw = gw / gw.sum()

rows = []
for i in range(N_APPLICATIONS):
    geo_idx = rng.choice(len(GEO), p=gw)
    provinsi, kabupaten, kecamatan, _ = GEO[geo_idx]
    name, gender = _name_and_gender(rng)
    age = int(np.clip(rng.normal(34, 8), 21, 60))
    dob = today - timedelta(days=age * 365 + int(rng.integers(0, 364)))
    nik = _nik_for_geo(rng, provinsi, dob, gender)
    ktp_text = _ktp_ocr(rng, nik, name, gender, dob, kabupaten)
    occ_cat, stability, occ_text = _occupation(rng, kabupaten)
    purpose = _purpose_text(rng, kabupaten)
    requested = int(np.clip(rng.normal(15_000_000, 25_000_000), 2_000_000, 200_000_000))
    requested = int(round(requested / 500_000)) * 500_000   # snap to 500k
    tenor = int(rng.choice([6, 12, 18, 24, 36], p=[.10,.30,.25,.25,.10]))
    channel = rng.choice(["APP","AGENT","BRANCH"], p=[.80,.15,.05])
    app_ts = today - timedelta(days=int(rng.integers(0, 90)),
                               hours=int(rng.integers(8, 21)),
                               minutes=int(rng.integers(0, 59)))
    existing_debt = int(np.clip(rng.gamma(1.2, 1_000_000), 0, 10_000_000))
    rows.append((
        100000 + i, name, gender, dob.date(), nik, ktp_text,
        f"08{rng.integers(1000000000, 9999999999)}",
        f"{name.split()[0].lower()}{rng.integers(80,99)}@gmail.com",
        occ_cat, stability, occ_text, purpose,
        requested, tenor, channel, app_ts,
        kecamatan, kabupaten, provinsi, existing_debt,
    ))

cols = ["application_id","applicant_name","gender","dob","nik","ktp_ocr_text",
        "phone","email","occupation_category_hidden","employment_stability_hidden",
        "occupation_freetext","purpose_freetext",
        "requested_amount_idr","tenor_months","channel","application_ts",
        "kecamatan","kabupaten","provinsi","monthly_existing_debt_idr"]
loan_app_pdf = pd.DataFrame(rows, columns=cols)

# Drop hidden labels from the bronze table — they're answer-keys we don't expose.
# But we'll save them off for later validation / discussion.
loan_app_pdf_bronze = loan_app_pdf.drop(columns=["occupation_category_hidden","employment_stability_hidden"])
print(f"  built {len(loan_app_pdf_bronze):,} applications")

# --- Build transactions -------------------------------------------------------
print(f"Generating ~50,000 transactions (avg 10 per applicant)...")

tx_rows = []
tx_id = 1
for app_id, app_ts in zip(loan_app_pdf_bronze["application_id"], loan_app_pdf_bronze["application_ts"]):
    n_tx = int(rng.integers(6, 16))  # 6-15 tx per applicant
    monthly_income = int(rng.uniform(4_000_000, 12_000_000))
    for _ in range(n_tx):
        days_back = int(rng.integers(1, 365))
        ts = app_ts - timedelta(days=days_back, hours=int(rng.integers(0,23)))
        ttype = rng.choice(["SALARY_IN","TRANSFER_IN","TRANSFER_OUT","ECOMMERCE","QRIS","UTILITY","ATM"],
                           p=[.10,.15,.20,.20,.15,.10,.10])
        if ttype == "SALARY_IN":
            amt = monthly_income + int(rng.normal(0, monthly_income * 0.05))
            desc = "GAJI BULANAN"
        elif ttype == "TRANSFER_IN":
            amt = int(rng.uniform(50_000, 5_000_000))
            desc = "TRF MASUK"
        elif ttype == "TRANSFER_OUT":
            amt = -int(rng.uniform(50_000, 3_000_000))
            desc = "TRF KELUAR"
        elif ttype == "ECOMMERCE":
            amt = -int(rng.uniform(20_000, 1_500_000))
            desc = rng.choice(["TOKOPEDIA","SHOPEE","TIKTOKSHOP","BUKALAPAK"])
        elif ttype == "QRIS":
            amt = -int(rng.uniform(15_000, 300_000))
            desc = "QRIS PMBYRN"
        elif ttype == "UTILITY":
            amt = -int(rng.uniform(80_000, 600_000))
            desc = rng.choice(["PLN","INDIHOME","BPJS","PDAM"])
        else:
            amt = -int(rng.uniform(100_000, 2_000_000))
            desc = "ATM TARIK"
        tx_rows.append((tx_id, int(app_id), ts, ttype, amt, desc))
        tx_id += 1
tx_pdf = pd.DataFrame(tx_rows,
    columns=["transaction_id","application_id","transaction_ts","transaction_type",
             "amount_idr","description"])
print(f"  built {len(tx_pdf):,} transactions")

# --- Build call notes ---------------------------------------------------------
print(f"Generating call-center notes...")

# Roughly 60% of applications have call notes; each has 1-3 notes
note_rows = []
note_id = 1
sampled = loan_app_pdf.sample(frac=0.6, random_state=SEED)
for _, row in sampled.iterrows():
    n_notes = int(rng.choice([1,2,3], p=[.45,.40,.15]))
    # The note sentiment is loosely correlated with the hidden employment_stability,
    # but with 25% noise (some stable applicants still get bad calls — that's the signal!)
    stab = row["employment_stability_hidden"]
    for k in range(n_notes):
        r = rng.random()
        if stab == "stable":
            bias = "good" if r < 0.65 else ("neutral" if r < 0.85 else "bad")
        elif stab == "variable":
            bias = "good" if r < 0.40 else ("neutral" if r < 0.70 else "bad")
        else:  # informal
            bias = "good" if r < 0.30 else ("neutral" if r < 0.50 else "bad")
        if bias == "good":
            text = rng.choice(CALL_GOOD)
        elif bias == "bad":
            text = rng.choice(CALL_BAD)
        else:
            text = rng.choice(CALL_NEUTRAL)
        agent = f"agent-{rng.integers(1001, 1099):04d}"
        ts = row["application_ts"] + timedelta(days=int(rng.integers(0, 3)),
                                                hours=int(rng.integers(9, 17)))
        note_rows.append((note_id, int(row["application_id"]), agent, ts, text))
        note_id += 1

note_pdf = pd.DataFrame(note_rows,
    columns=["note_id","application_id","agent_id","note_ts","note_text"])
print(f"  built {len(note_pdf):,} call notes")

# COMMAND ----------

# MAGIC %md ## Write to bronze

# COMMAND ----------

(spark.createDataFrame(loan_app_pdf_bronze)
   .write.format("delta").mode("overwrite").option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.bronze.loan_application"))
(spark.createDataFrame(tx_pdf)
   .write.format("delta").mode("overwrite").option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.bronze.transaction"))
(spark.createDataFrame(note_pdf)
   .write.format("delta").mode("overwrite").option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.bronze.call_note"))

for t in ("loan_application","transaction","call_note"):
    cnt = spark.table(f"{CATALOG}.bronze.{t}").count()
    print(f"  ✓ {CATALOG}.bronze.{t}: {cnt:,} rows")

# COMMAND ----------

# MAGIC %md ## Peek at the data
# MAGIC
# MAGIC Take a moment to look at the messy free-text fields. This is what the next notebook will untangle with `ai_extract`.

# COMMAND ----------

display(spark.sql(f"""
SELECT application_id, applicant_name, kabupaten, requested_amount_idr,
       SUBSTRING(ktp_ocr_text, 1, 80)        AS ktp_preview,
       SUBSTRING(occupation_freetext, 1, 80) AS occupation_preview
FROM {CATALOG}.bronze.loan_application
ORDER BY application_ts DESC
LIMIT 10
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT note_id, application_id, SUBSTRING(note_text, 1, 100) AS note_preview
FROM {CATALOG}.bronze.call_note
ORDER BY note_ts DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup complete
# MAGIC
# MAGIC You now have:
# MAGIC
# MAGIC | Table | Rows | What it holds |
# MAGIC |---|---|---|
# MAGIC | `workshop.bronze.loan_application` | ~5,000 | Application form including messy KTP OCR text + Bahasa free-text occupation/purpose fields |
# MAGIC | `workshop.bronze.transaction` | ~50,000 | 12 months of bank-statement transactions per applicant |
# MAGIC | `workshop.bronze.call_note` | ~9,000 | Call-center verification notes in Bahasa/English |
# MAGIC
# MAGIC **Next:** open **`01_ai_extract_kyc.py`** — we'll use `ai_extract` to turn that messy text into structured KYC fields.
