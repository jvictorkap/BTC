a = """B          VALE3 1,100    79.1500
B          PETR4 1,500   29.7900
B          PETR3 1,200   31.6300
B          ITUB4  1,500   21.7700
B          BBDC4 1,400   20.2700
B          B3SA3 2,000  12.4000
B          RENT3 400     60.1200
B          ABEV3 1,400   16.1100
B          GNDI3 300      69.5600
S          IGTI11  100      195.0000
B          BBAS3 600     31.7200
B          JBSS3  500      38.0100
B          PETZ3 900     19.6200
B          ITSA4  1,500   9.7200
B          WEGE3           300     35.9700
B          RADL3 400     24.1400
B          BPAC11           400     23.0700
B          GGBR4 300      29.5200
B          LREN3 300      27.1100
S          EGIE3  200     39.2400
B          ENEV3 500      14.6500
B          BIDI11  200     36.0900
B          MGLU3            1,100   6.3800
B          BBDC3 400     17.2400
B          VBBR3 300      22.6700
B          CSAN3 300      22.5400
B          SANB11           200     32.2900
B          HAPV3 500      12.3000
B          SUZB3 100      59.0200
B          RAIL3  300      18.8200
B          CMIG4 400     13.8900
S          BRAP4 100      55.5400
B          LCAM3            200     27.1300
B          SULA11            200     26.7800
B          KLBN11            200     25.7400
B          CCRO3 400     12.6200
S          VIVT3  100      48.7000
B          RDOR3 100      47.8100
B          USIM5 300      15.6800
S          PCAR3 200     23.3500
B          UGPA3 300      15.4100
B          BBSE3 200     21.1400
B          EMBR3 200     20.8800
B          BRFS3 200     20.4800
B          PRIO3  200     20.2400
S          SBSP3 100      39.4000
B          TAEE11 100      36.4600
B          ELET3 100      34.0400
S          ELET6 100      33.7300
B          TOTS3 100      32.3900
B          CPLE6 500      6.3800
B          CRFB3 200     15.7300
B          AMER3            100      29.7500
B          NTCO3            100      26.6500
B          CSNA3 100      25.0800
B          GOAU4           200     12.2000
B          YDUQ3            100      24.4000
B          EQTL3 100      23.4200
B          IRBR3  500      4.4400
B          EZTC3 100      21.0800
B          ENBR3 100      21.0100
S          GOLL4 100      18.7900
B          QUAL3 100      17.3300
S          DXCO3            100      16.3300
B          CYRE3 100      16.0500
B          VIIA3   300      5.2600
B          ASAI3  100      14.8000
B          COGN3            500     2.6500
B          TIMS3 100      13.2300
B          MRVE3            100      12.1400
S          BIDI4   100      11.8500
S          BEEF3 100      9.5700
B          BRML3 100      8.2400
B          ECOR3 100      8.1500
B          CIEL3  300      2.3300
S          GETT11            100      3.8100
B          CASH3 100      3.4400
"""
import pandas as pd

lines = a.split("\n")
df = pd.DataFrame()
datas = []
for line in lines:
    chars = line.split(" ")
    data = []
    for char in chars:
        if char == "":
            continue
        else:
            try:
                data.append(float(char.replace(",", "")))
            except:
                data.append(char.replace(",", ""))
    datas.append(data)

df = pd.DataFrame(datas)
df.columns = ["SIDE", "TICKER", "QTY", "PRICE"]
df = df[df["SIDE"].notna()]
df["quantity"] = df.apply(
    lambda row: -row["QTY"] if row["SIDE"] == "S" else row["QTY"], axis=1
)
df["CODIGO"] = df["TICKER"].apply(lambda x: x[:-1] if len(x) == 5 else x[:-2])
df["SERIE"] = df["TICKER"].apply(lambda x: x + " BZ EQUITY")
print(df)
df.to_excel("bol.xlsx")
