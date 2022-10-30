# load libraries
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 500)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


#########################################################################################################################
#########################################################################################################################
# examine the features

# master_id : Unique customer number
# order_channel : Which channel of the shopping platform is used (Android, ios, Desktop, Mobile)
# last_order_channel : The channel with the last shopping
# first_order_date : Date of the customer's first purchase
# last_order_date :	Date of the customer's last purchase
# last_order_date_online : The date of the last purchase made by the customer on the online platform
# last_order_date_offline : The date of the last purchase made by the customer on the offline platform
# order_num_total_ever_online :	The total number of purchases made by the customer on the online platform
# order_num_total_ever_offline : Total number of purchases made by the customer offline
# customer_value_total_ever_offline : Total fee paid by the customer for offline purchases
# customer_value_total_ever_online : The total fee paid by the customer for their online shopping
# interested_in_categories_12 :	List of categories the customer has shopped in the last 12 months
#########################################################################################################################
#########################################################################################################################

# load data and create a copy
df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()

# Examine the first 10 observations, variable names, descriptive statistics, missing values, variable types
def general_info(dataframe):
    print("First 10 observations")
    print("################################")
    print(dataframe.head(10),end="\n\n")
    print("Variable names")
    print("################################")
    print(dataframe.columns, end= "\n\n")
    print("Descriptive statistics")
    print("################################")
    print(dataframe.describe().T, end="\n\n")
    print("Missing values")
    print("################################")
    print(dataframe.isnull().sum(),end="\n\n")
    print("Variable types")
    print("################################")
    print(dataframe.info())

general_info(df)


# Omnichannel means that customers shop both online and offline.
# Create new variables for the total number of purchases and spending for each customer.
df["order_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

# examine the features types
df.info()
df.head(3)

# convert the date variables to date type
df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

# look at the distrubution of the number of customers, total number of products and total spending by shopping channel
df.groupby("order_channel").agg({"master_id":"nunique",
                                 "order_num_total_ever":"sum",
                                 "customer_value_total_ever":"sum"})


# sort the customers by their total spending and examine the first 10 customers
df.master_id.duplicated().sum() # 0
df.sort_values("customer_value_total_ever", ascending=False).head(10)
#df.groupby("master_id").agg({"customer_value_total_ever": "sum"}).sort_values("customer_value_total_ever", ascending=False).head(10)


# sort the customers by their total number of purchases and examine the first 10 customers
df.sort_values("order_num_total_ever", ascending=False).head(10)


# create a function for data preparation
path = "flo_data_20k.csv"

def preprocessing(path):
    # Step 1
    df_ = pd.read_csv(path)
    df = df_.copy()

    # Step 2
    print("Overview")
    print("################################################################")
    print("First 10 observations")
    print("################################################################")
    print(df.head(10), end="\n\n")
    print("Variable names")
    print("################################################################")
    print(df.columns, end="\n\n")
    print("Descriptive statistics")
    print("################################################################")
    print(df.describe().T, end="\n\n")
    print("Missing values")
    print("################################################################")
    print(df.isnull().sum(), end="\n\n")
    print("Variable types")
    print("################################################################")
    print(df.info(),end="\n\n")

    # Step 3
    df["order_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

    # Step 4
    for i in df.columns:
        if 'date' in i:
            df[i] = pd.to_datetime(df[i])

    # Step 5
    print("Distribution of the number of customers, total number of products and total spending by shopping channel")
    print("################################################################")
    print(df.groupby("order_channel").agg({"master_id":"nunique",
                                           "order_num_total_ever":"sum",
                                           "customer_value_total_ever":"sum"}),end="\n\n")

    # Step 6
    print("Top 10 customers by total spending")
    print("################################")
    print(df.sort_values("customer_value_total_ever", ascending=False).head(10),end="\n\n")

    # Step 7
    print("Top 10 customers by total number of purchases")
    print("################################################################")
    print(df.sort_values("order_num_total_ever", ascending=False).head(10))

    return df

df = preprocessing(path)


#########################################################################################################################
#########################################################################################################################

# recency : time since last purchase
# frequency : total number of purchases
# monetary : total spending

# select the date of analysis as the day after the maximum date
df.last_order_date.max() # Timestamp('2021-05-30 00:00:00')
analysis_date = df.last_order_date.max() + pd.DateOffset(days=2)


# Recency = analysis_date - last_order_date
# frequency = order_num_total_ever
# monetary = customer_value_total_ever

# calculate recency, frequency and monetary for each customer
df.groupby("master_id").agg({"last_order_date": lambda date: (analysis_date - date.max()).days,
                                    "order_num_total_ever": lambda order: order.count(),
                                    "customer_value_total_ever": lambda price: price.sum()})


#Assign your calculated metrics to a variable named rfm and  change the names of the columns to recency, frequency and monetary.
rfm = df.groupby("master_id").agg({"last_order_date": lambda date: (analysis_date - date.max()).days,
                                    "order_num_total_ever": lambda order: order.count(),
                                    "customer_value_total_ever": lambda price: price.sum()})

rfm.columns = ["recency", "frequency", "monetary"]
#########################################################################################################################
#########################################################################################################################

# convert recency, frequency and monetary to scores between 1-5 using qcut function
# record these scores as recency_score, frequency_score and monetary_score
rfm["recent_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

# record the recency_score and frequency_score as a single variable RF_SCORE
rfm["RF_SCORE"] = (rfm["recent_score"].astype(str) + rfm["frequency_score"].astype(str))

#########################################################################################################################
#########################################################################################################################

# description of the segments for RFM analysis
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

# convert the RF_SCORE to segments using the seg_map
rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

#########################################################################################################################
#########################################################################################################################

# examine the average recency, frequency and monetary values for each segment
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean"])


# FLO includes a new women's shoe brand. The price of the products included in the brand is above the general customer preferences.
# For this reason, it is desired to communicate with the customers who will be interested in the promotion and sales of the product.
# Special communication will be made with customers who are loyal customers (champions, loyal_customers)

type1 = rfm[(rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")]
type2 = df[(df["interested_in_categories_12"]).str.contains("KADIN")]

id1 = pd.merge(type1, type2, on=["master_id"]).master_id

# save to csv file
id1.to_csv("case1_id.csv", index=False)


# nearly 40% discount is planned for men and children products.
# It is planned to target customers who have not purchased for a long time, the customers who are about to sleep and new customers.

customer1 = rfm[(rfm["segment"] == "cant_loose") | (rfm["segment"] == "about_to_sleep") | (rfm["segment"] == "new_customers")]

customer2 = df[(df["interested_in_categories_12"]).str.contains("ERKEK | COCUK")]

id2 = pd.merge(customer1, customer2, on=["master_id"]).master_id

# save to csv file
id2.to_csv("case2_id.csv", index=False)
