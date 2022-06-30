# Task 1

## Recipes data exploration

### Download dataset and install dependencies


```python
# !wget https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json -O /tmp/recipes.json
# !pip install -U pandas openpyxl
```

### Import dependencies


```python
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from functools import reduce
from datetime import date
```

### Configure SparkSession


```python
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
```

### First impressions of dataset


```python
input_df = spark.read.json('/tmp/recipes.json')
input_df
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>PT</td><td>2013-04-01</td><td>Got leftover East...</td><td>http://static.the...</td><td>12 whole Hard Boi...</td><td>Easter Leftover S...</td><td>PT15M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2011-06-06</td><td>I finally have ba...</td><td>http://static.the...</td><td>3/4 cups Fresh Ba...</td><td>Pasta with Pesto ...</td><td>PT6M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2011-09-15</td><td>This was yummy. A...</td><td>http://static.the...</td><td>2 whole Pork Tend...</td><td>Herb Roasted Pork...</td><td>PT5M</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>PT20M</td><td>2012-04-23</td><td>I made this for a...</td><td>http://static.the...</td><td>1 pound Penne
4 w...</td><td>Chicken Florentin...</td><td>PT10M</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>PT</td><td>2011-06-13</td><td>Iced coffee is my...</td><td>http://static.the...</td><td>1 pound Ground Co...</td><td>Perfect Iced Coffee</td><td>PT8H</td><td>24</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-05-31</td><td>When I was in Alb...</td><td>http://static.the...</td><td>1 whole Onion, Di...</td><td>Easy Green Chile ...</td><td>PT5M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT5M</td><td>2013-03-25</td><td>Imagine the Easte...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Krispy Easter Eggs</td><td>PT20M</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>PT25M</td><td>2012-08-06</td><td>Who doesn&#x27;t love ...</td><td>http://static.the...</td><td>1 stick Butter
1 ...</td><td>Patty Melts</td><td>PT10M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT2M</td><td>2012-08-10</td><td>Note from PW: On ...</td><td>http://static.the...</td><td> Doughnuts
1-1/8 ...</td><td>Yum. Doughnuts!</td><td>PT25M</td><td>18</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2012-08-01</td><td>This is just a qu...</td><td>http://static.the...</td><td>1 pound Pasta (fe...</td><td>Buttery Lemon Par...</td><td>PT5M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H15M</td><td>2012-08-13</td><td>There&#x27;s nothing s...</td><td>http://static.the...</td><td>1 whole Chicken, ...</td><td>Roast Chicken</td><td>PT10M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H</td><td>2012-08-18</td><td>On this morning&#x27;s...</td><td>http://static.the...</td><td> FRENCH TOAST
 Bu...</td><td>Baked French Toast</td><td>PT15M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-08-15</td><td>I love slice-and-...</td><td>http://static.the...</td><td>2-1/2 cups All-pu...</td><td>Yummy Slice-and-B...</td><td>PT15M</td><td>30</td><td>http://thepioneer...</td></tr>
<tr><td>PT20M</td><td>2012-08-20</td><td>I love grilled ve...</td><td>http://static.the...</td><td>6 whole Zucchini ...</td><td>Yummy Grilled Zuc...</td><td>PT30M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-08-29</td><td>When we went to V...</td><td>http://static.the...</td><td>16 whole Graham C...</td><td>Chocolate Covered...</td><td>PT20M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT5M</td><td>2012-08-22</td><td>I love compound b...</td><td>http://static.the...</td><td> HOTEL BUTTER
2 s...</td><td>T-Bone Steaks wit...</td><td>PT20M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT</td><td>2012-08-31</td><td> I&#x27;ve got Mango M...</td><td>http://static.the...</td><td>2 whole Limes
2 T...</td><td>Mango Margaritas!</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT45M</td><td>2012-01-04</td><td>I&#x27;m Pioneer Woman...</td><td>http://static.the...</td><td>5 cloves Garlic, ...</td><td>Tuscan Bean Soup ...</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H</td><td>2011-12-30</td><td>I&#x27;m not a big bel...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Hoppin’ John</td><td>PT6H</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2012-01-11</td><td>I would like to s...</td><td>http://static.the...</td><td>8 whole Everythin...</td><td>Turkey Bagel Burger</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
</table>
only showing top 20 rows





```python
input_df.printSchema()
```

    root
     |-- cookTime: string (nullable = true)
     |-- datePublished: string (nullable = true)
     |-- description: string (nullable = true)
     |-- image: string (nullable = true)
     |-- ingredients: string (nullable = true)
     |-- name: string (nullable = true)
     |-- prepTime: string (nullable = true)
     |-- recipeYield: string (nullable = true)
     |-- url: string (nullable = true)
    



```python
input_df.describe()
```




<table border='1'>
<tr><th>summary</th><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>count</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td></tr>
<tr><td>mean</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>9.738404452690167</td><td>null</td></tr>
<tr><td>stddev</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>6.529901914334942</td><td>null</td></tr>
<tr><td>min</td><td></td><td>2003-05-27</td><td>      IMPORTANT: ...</td><td>http://static.the...</td><td> (Quantities Depe...</td><td></td><td></td><td></td><td>http://thepioneer...</td></tr>
<tr><td>max</td><td>PT9M</td><td>2013-04-01</td><td>   We just had a ...</td><td>http://www.101coo...</td><td>~1 1/2 cups (7 ou...</td><td>Zucchini Ricotta ...</td><td>PT950M</td><td>Serves six.</td><td>http://www.101coo...</td></tr>
</table>




### Now that I know the names and types of the dataset fields and the null count, I can enforce a simple schema


```python
schema = StructType([
    StructField('cookTime', StringType()),
    StructField('datePublished', StringType()),
    StructField('description', StringType()),
    StructField('image', StringType()),
    StructField('ingredients', StringType()),
    StructField('name', StringType()),
    StructField('prepTime', StringType()),
    StructField('recipeYield', StringType()),
    StructField('url', StringType()),
])

input_df = spark.read.json('/tmp/recipes.json', schema=schema)
input_df.createOrReplaceTempView('raw')
```

### Validating if 'datePublished' can be converted to date


```python
spark.sql("SELECT count_if(to_date(datePublished) IS NULL) FROM raw")
```




<table border='1'>
<tr><th>count_if((to_date(raw.`datePublished`) IS NULL))</th></tr>
<tr><td>0</td></tr>
</table>





```python
spark.sql("SELECT min(to_date(datePublished)), max(to_date(datePublished)) FROM raw")
```




<table border='1'>
<tr><th>min(to_date(raw.`datePublished`))</th><th>max(to_date(raw.`datePublished`))</th></tr>
<tr><td>2003-05-27</td><td>2013-04-01</td></tr>
</table>




### Validating if 'url' and 'image' have a valid URL format


```python
spark.sql(r"""
    SELECT count_if(url NOT RLIKE '(https?)://(?:[^/$.?#]|\S).\S*') AS url,
           count_if(image NOT RLIKE '(https?)://(?:[^/$.?#]|\S).\S*') AS image 
    FROM raw
""")
```




<table border='1'>
<tr><th>url</th><th>image</th></tr>
<tr><td>0</td><td>0</td></tr>
</table>




### Validating if 'name' column has any empty value


```python
spark.sql("SELECT * FROM raw WHERE length(name) = 0")
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td></td><td>2003-05-27</td><td>101 Cookbooks: Pe...</td><td>http://www.101coo...</td><td>Salt
One 35-ounce...</td><td></td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2006-01-07</td><td>A twist on a clas...</td><td>http://www.101coo...</td><td>onions - 4 medium...</td><td></td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT45M</td><td>2009-10-05</td><td>In the realm of g...</td><td>http://www.101coo...</td><td>1 quart (4 cups) ...</td><td></td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
</table>




These three empty occurrences can be filled based on the values of description, image and url

### Validating if 'ingredients' can be converted to array[string\]


```python
spark.sql("""
    SELECT explode(split(ingredients, '\n'))
    FROM raw
    ORDER BY 1 DESC
""")
```




<table border='1'>
<tr><th>col</th></tr>
<tr><td> For the glaze:</td></tr>
<tr><td> Fine-grain sea s...</td></tr>
<tr><td> 5 1/2 cups / 1.3...</td></tr>
<tr><td> 3/4 teaspoon bak...</td></tr>
<tr><td> 3/4 cup / 180 ml...</td></tr>
<tr><td> 3 large eggs, at...</td></tr>
<tr><td> 3 cloves garlic,...</td></tr>
<tr><td> 2/3 cup / 3 oz /...</td></tr>
<tr><td> 2 large eggs, li...</td></tr>
<tr><td> 1/4 teaspoon fin...</td></tr>
<tr><td> 1/4 cup fresh pa...</td></tr>
<tr><td> 1/4 cup (small h...</td></tr>
<tr><td> 1/3 cup / 80 ml ...</td></tr>
<tr><td> 1/2 teaspoon van...</td></tr>
<tr><td> 1/2 teaspoon fin...</td></tr>
<tr><td> 1/2 cup / 3 oz /...</td></tr>
<tr><td> 1/2 cup / 2 oz /...</td></tr>
<tr><td> 1/2 cup / 125 ml...</td></tr>
<tr><td> 1/2 cup / 120 ml...</td></tr>
<tr><td> 1/2 cup  / 4 oz ...</td></tr>
</table>
only showing top 20 rows





```python
spark.sql("""
    SELECT explode(split(ingredients, '\n'))
    FROM raw
    ORDER BY 1
""")
```




<table border='1'>
<tr><th>col</th></tr>
<tr><td></td></tr>
<tr><td></td></tr>
<tr><td> </td></tr>
<tr><td> </td></tr>
<tr><td> </td></tr>
<tr><td>  </td></tr>
<tr><td>  </td></tr>
<tr><td>   Cook time: 10 ...</td></tr>
<tr><td>   Cook time: 45 ...</td></tr>
<tr><td> (Quantities Depe...</td></tr>
<tr><td> (add 1/2 Teaspoo...</td></tr>
<tr><td> **Slightly Adapt...</td></tr>
<tr><td> - my favorite fi...</td></tr>
<tr><td> 1 Or 2 Slices Of...</td></tr>
<tr><td> 1 tablespoon ext...</td></tr>
<tr><td> 1/2 Cup Olive Oil</td></tr>
<tr><td> 1/2 Hot Chili Pe...</td></tr>
<tr><td> 1/2 Jalapeno, Se...</td></tr>
<tr><td> 1/2 Red Onion, D...</td></tr>
<tr><td> 1/2 Teaspoon Red...</td></tr>
</table>
only showing top 20 rows




For the subjectiveness of the ingredients column, it is better to keep it as a string

### Validating if 'recipeYield' can be converted to integer


```python
spark.sql("SELECT count(1) FROM raw WHERE CAST(recipeYield AS INT) IS NULL")
```




<table border='1'>
<tr><th>count(1)</th></tr>
<tr><td>503</td></tr>
</table>





```python
spark.sql("SELECT * FROM raw WHERE CAST(recipeYield AS INT) IS NULL")
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td></td><td>2009-07-06</td><td>From the Big Sur ...</td><td>http://www.101coo...</td><td>5 cups all-purpos...</td><td>Big Sur Bakery Hi...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT30M</td><td>2009-08-27</td><td>An old-fashioned ...</td><td>http://www.101coo...</td><td>1 cup plus 2 tabl...</td><td>Old-Fashioned Blu...</td><td>PT10M</td><td>Serves  8 - 10.</td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2009-10-25</td><td>An apple and carr...</td><td>http://www.101coo...</td><td>1/4 cup / 2 ounce...</td><td>Apple and Carrot ...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT15M</td><td>2009-11-15</td><td>Rustic orange-sce...</td><td>http://www.101coo...</td><td>3 cups whole whea...</td><td>Orange and Oat Sc...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT10M</td><td>2009-12-04</td><td>I made these for ...</td><td>http://www.101coo...</td><td>1/2 cup / 3.5 oz ...</td><td>Sparkling Ginger ...</td><td>PT30M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2009-12-09</td><td>This olive oil fl...</td><td>http://www.101coo...</td><td>4 1/2 cups / 1 lb...</td><td>Seeded Flatbread ...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT45M</td><td>2010-03-10</td><td>These jammy, fig-...</td><td>http://www.101coo...</td><td>Dry mix:
1 cup / ...</td><td>Figgy Buckwheat S...</td><td>PT100M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT25M</td><td>2010-03-30</td><td>These muffins are...</td><td>http://www.101coo...</td><td>butter to grease ...</td><td>Lucia Muffins</td><td>PT20M</td><td>Makes 10 - 12 muf...</td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2010-04-10</td><td>Dense, gooey choc...</td><td>http://www.101coo...</td><td>butter for greasi...</td><td>Chocolate Cherry ...</td><td>PT900M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2010-04-24</td><td>A rustic, minimal...</td><td>http://www.101coo...</td><td>butter to grease ...</td><td>Quinoa Skillet Bread</td><td>PT10M</td><td>Makes one 10 1/2 ...</td><td>http://www.101coo...</td></tr>
<tr><td>PT40M</td><td>2010-05-04</td><td>A simple spring c...</td><td>http://www.101coo...</td><td>butter for greasi...</td><td>Strawberry Rhubar...</td><td>PT20M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT15M</td><td>2010-05-14</td><td>Cookies made from...</td><td>http://www.101coo...</td><td>3/4 cup / 3.5 oz ...</td><td>Quinoa Cloud Cookies</td><td>PT60M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT45M</td><td>2010-05-23</td><td>A cake I made for...</td><td>http://www.101coo...</td><td>Olive oil for the...</td><td>Rosemary Olive Oi...</td><td>PT15M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT50M</td><td>2010-06-27</td><td>Certainly not the...</td><td>http://www.101coo...</td><td>zest of one lemon...</td><td>Chocolate Loaf Cake</td><td>PT15M</td><td>Makes 8 - 10 slices.</td><td>http://www.101coo...</td></tr>
<tr><td>PT40M</td><td>2010-05-31</td><td>Inspired by Hugh ...</td><td>http://www.101coo...</td><td>2 1/2 tablespoons...</td><td>Six-seed Soda Bre...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2003-05-27</td><td>101 Cookbooks: Pe...</td><td>http://www.101coo...</td><td>Salt
One 35-ounce...</td><td></td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2003-06-29</td><td>101 Cookbooks: Sc...</td><td>http://www.101coo...</td><td>6 ounces Scharffe...</td><td>Scharffen Berger ...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-09-06</td><td>101 Cookbooks: Ch...</td><td>http://www.101coo...</td><td>1 lb. Jacob&#x27;s Cat...</td><td>Chocolate Calypso...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-10-20</td><td>101 Cookbooks: Ap...</td><td>http://www.101coo...</td><td>3 tablespoons all...</td><td>Apple Pie Recipe</td><td></td><td>Serves 6 to 8
Act...</td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-11-10</td><td>Nancy Silverton&#x27;s...</td><td>http://www.101coo...</td><td>2 1/2 cups plus 2...</td><td>Graham Cracker Re...</td><td></td><td></td><td>http://www.101coo...</td></tr>
</table>
only showing top 20 rows




### Almost half of 'recipeYield' values are not possible to cast to integer

Analyzing the dataset, this column can sometimes have a sentence that describes the recipe's yields, or even does not have any info at all. For this absence of info, I can consider a null value, for the other ones, it is better to further the column analysis.


```python
spark.sql("SELECT recipeYield FROM raw WHERE CAST(recipeYield AS INT) IS NULL AND length(recipeYield) > 0").show(20, False)
```

    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |recipeYield                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |Serves  8 - 10.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
    |Makes 10 - 12 muffins (12 in the Lodge cast iron muffin pans)...                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
    |Makes one 10 1/2 skillet.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    |Makes 8 - 10 slices.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
    |Serves 6 to 8
    Active time: 40 minutes
    Start to finish: 5 1/2 hours (includes making dough and cooling)Put a large baking sheet on middle oven rack and preheat oven to 425F.Whisk together flour, zest, cinnamon, allspice, salt, and 2/3 cup sugar in a large bowl. Gently toss with apples and lemon juice.Roll out 1 piece of dough (keep remaining piece chilled) on a lightly floured surface with a lightly floured rolling pin into a 13-inch round. Fit it into a 9-inch pie plate. Trim edge, leaving a 1/2-inch overhang. Refrigerate while you roll out dough for top crust.Roll out remaining piece of dough on lightly floured surface into an 11-inch round.Spoon filling into shell. Cover pie with pastry round and trim with kitchen shears, leaving 1/2-inch overhang. Press edges together, then crimp decoratively. Lightly brush top of pie with egg and sprinkle all over with remaining 1 tablespoon sugar.  With a small sharp knife, cut 3 vents in top crust.Bake Pie on hot baking sheet for 20 minutes. Reduce oven temperature to 375F and continue to bake until crust is golden and filling is bubbling, about 40 minutes more. Cool pie on a rack to warm or room temperature, 2 to 3 hours.Makes enough for a single crust 9-inch pie or a 9- to 11-inch tart, or for a double crust 9-inch pie.
    Active time: 10 minutes
    Start to finish: 1 1/4 hours (includes chilling)For a single crust pie or a tartFor a double-crust pieBlend together flour, butter, shortening, and salt in a bowl with your fingers or a pastry blender (or pulse in a food processor) just until mixture resembles coarse meal with some small (roughly pea sized) lumps of butter. For a single crust pie or tart, drizzle 3 tablespoons ice water evenly over mixture and gently stir with a fork (or pulse) until incorporated.Squeeze a small handful of dough: if it doesn't hold together, ad more ice water 1/2 tablespoon at a time, stirring or pulsing until incorporated. Do not overwork dough or pastry will be tough.Turn dough out onto a work surface. For a single-crust pie or tart, divide into 4 portions; for a double-crust pie, divide into 8 portions. With heel of your hand, smear each portion once or twice in a forward motion to help distribute fat. Gather all dough together, with a pastry scraper if you have one. For single-crust pie or tart, press into a ball, then flatten into a 5-inch disk. For a double-crust pie, divide in half, and then flatten each into a 5-inch disk. If dough is sticky, dust lightly with additional flour. Wrap each disk in plastic wrap and refrigerate until firm, at least one hour.Cook's note: 
    The dough can be refrigerated for up to 1 day.From The Gourmet Cookbook edited by Ruth Reichl (Houghton Mifflin Company, 2004)Print Recipe|
    |Makes 12 canneles.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
    |Serves 4 to 6.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
    |Makes about 6 side servings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
    |Serves 8 as a first course, or 4 to 6 as a main course.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
    |Serves 4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    |Makes 9 scones.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
    |Serves  6 - 8.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
    |Makes about 2 dozen small rice balls.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
    |Serves 2-3 as a main.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
    |Serves about 6-8.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
    |Serves 6 (or so) as a side.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
    |Serves 2 - 4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
    |Makes 4-6 servings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
    |Makes about 6 servings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
    |Serves 4 I Braising Time: about 1 1/2 hours.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    only showing top 20 rows
    


There is no good way to infer an integer value to 'recipeYield' value

### Validating if 'cookTime' and 'prepTime' can be converted to integer


```python
spark.sql("SELECT distinct cookTime FROM raw")
```




<table border='1'>
<tr><th>cookTime</th></tr>
<tr><td>PT2M</td></tr>
<tr><td>PT9M</td></tr>
<tr><td>PT22M</td></tr>
<tr><td>PT55M</td></tr>
<tr><td>PT15M</td></tr>
<tr><td>PT1H</td></tr>
<tr><td>PT5M</td></tr>
<tr><td>PT1H10M</td></tr>
<tr><td>PT6M</td></tr>
<tr><td>PT8M</td></tr>
<tr><td>PT3M</td></tr>
<tr><td>PT</td></tr>
<tr><td>PT18M</td></tr>
<tr><td>PT40M</td></tr>
<tr><td>PT20M</td></tr>
<tr><td>PT1H15M</td></tr>
<tr><td>PT6H</td></tr>
<tr><td>PT23M</td></tr>
<tr><td>PT60M</td></tr>
<tr><td>PT12M</td></tr>
</table>
only showing top 20 rows





```python
spark.sql("SELECT distinct prepTime FROM raw")
```




<table border='1'>
<tr><th>prepTime</th></tr>
<tr><td>PT2M</td></tr>
<tr><td>PT15M</td></tr>
<tr><td>PT1H</td></tr>
<tr><td>PT24H</td></tr>
<tr><td>PT950M</td></tr>
<tr><td>PT5M</td></tr>
<tr><td>PT6M</td></tr>
<tr><td>PT3M</td></tr>
<tr><td>PT</td></tr>
<tr><td>PT1M</td></tr>
<tr><td>PT40M</td></tr>
<tr><td>PT20M</td></tr>
<tr><td>PT6H</td></tr>
<tr><td>PT1H15M</td></tr>
<tr><td>PT60M</td></tr>
<tr><td>PT4H</td></tr>
<tr><td>PT18H</td></tr>
<tr><td>PT900M</td></tr>
<tr><td>PT65M</td></tr>
<tr><td>PT35M</td></tr>
</table>
only showing top 20 rows





```python
spark.sql(r"""
    SELECT 'prepTime' AS source, prepTime RLIKE 'PT(?:\d+H)?(?:\d+M)?', count(1) FROM raw GROUP BY 1, 2
    UNION  
    SELECT 'cookTime' AS source, cookTime RLIKE 'PT(?:\d+H)?(?:\d+M)?', count(1) FROM raw GROUP BY 1, 2
""")
```




<table border='1'>
<tr><th>source</th><th>prepTime RLIKE PT(?:d+H)?(?:d+M)?</th><th>count(1)</th></tr>
<tr><td>prepTime</td><td>true</td><td>739</td></tr>
<tr><td>cookTime</td><td>true</td><td>716</td></tr>
<tr><td>prepTime</td><td>false</td><td>303</td></tr>
<tr><td>cookTime</td><td>false</td><td>326</td></tr>
</table>




Both of these columns are usually on **PT(\d+H)?(\d+M)?** pattern, which indicates hours and minutes to prepare or cook a dish. 

- Visiting some recipes on Pioneer Woman website, the absence of a time info indicates that a dish does not need to be cooked or prepared.
- Visiting some recipes on 101 Cookbooks website, the absence of a time info does not necessary indicates a the dish does not need to be cooked or prepared. It can indicate just the absence of the info at all.


```python
spark.sql(r"""
    SELECT regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) AS domain, 'prepTime' AS source, prepTime RLIKE 'PT(?:\d+H)?(?:\d+M)?', count(1) FROM raw GROUP BY 1, 2, 3
    UNION  
    SELECT regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) AS domain, 'cookTime' AS source, cookTime RLIKE 'PT(?:\d+H)?(?:\d+M)?', count(1) FROM raw GROUP BY 1, 2, 3
    ORDER BY 1, 2
""")
```




<table border='1'>
<tr><th>domain</th><th>source</th><th>prepTime RLIKE PT(?:d+H)?(?:d+M)?</th><th>count(1)</th></tr>
<tr><td>101cookbooks.com</td><td>cookTime</td><td>true</td><td>177</td></tr>
<tr><td>101cookbooks.com</td><td>cookTime</td><td>false</td><td>326</td></tr>
<tr><td>101cookbooks.com</td><td>prepTime</td><td>true</td><td>200</td></tr>
<tr><td>101cookbooks.com</td><td>prepTime</td><td>false</td><td>303</td></tr>
<tr><td>thepioneerwoman.com</td><td>cookTime</td><td>true</td><td>539</td></tr>
<tr><td>thepioneerwoman.com</td><td>prepTime</td><td>true</td><td>539</td></tr>
</table>




This pattern can be used to fully convert the time columns of Pioneer Woman website, but only partially from 101 Cookbooks website


```python
spark.sql(r"""
    SELECT source RLIKE '(PT(?:\d+H)?(?:\d+M)?)' AS test, collect_list(source)
    FROM (
        SELECT prepTime AS source FROM raw 
        WHERE regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) = '101cookbooks.com'
        UNION
        SELECT cookTime AS source FROM raw 
        WHERE regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) = '101cookbooks.com'
    ) 
    GROUP BY 1
""").show(2, False)
```

    +-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |test |collect_list(source)                                                                                                                                                                     |
    +-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |true |[PT2M, PT15M, PT950M, PT5M, PT8M, PT3M, PT40M, PT20M, PT60M, PT50M, PT900M, PT65M, PT70M, PT35M, PT100M, PT150M, PT180M, PT245M, PT90M, PT240M, PT45M, PT10M, PT30M, PT25M, PT120M, PT7M]|
    |false|[]                                                                                                                                                                                       |
    +-----+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    



```python
spark.sql(r"""
    SELECT source, count(1)
    FROM (
        SELECT prepTime AS source FROM raw 
        WHERE regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) = '101cookbooks.com'
        UNION ALL
        SELECT cookTime AS source FROM raw 
        WHERE regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) = '101cookbooks.com'
    ) 
    GROUP BY 1
    ORDER BY 2 DESC
""")
```




<table border='1'>
<tr><th>source</th><th>count(1)</th></tr>
<tr><td></td><td>629</td></tr>
<tr><td>PT10M</td><td>105</td></tr>
<tr><td>PT5M</td><td>67</td></tr>
<tr><td>PT15M</td><td>38</td></tr>
<tr><td>PT20M</td><td>35</td></tr>
<tr><td>PT30M</td><td>24</td></tr>
<tr><td>PT60M</td><td>23</td></tr>
<tr><td>PT45M</td><td>17</td></tr>
<tr><td>PT25M</td><td>15</td></tr>
<tr><td>PT40M</td><td>8</td></tr>
<tr><td>PT35M</td><td>6</td></tr>
<tr><td>PT120M</td><td>5</td></tr>
<tr><td>PT240M</td><td>4</td></tr>
<tr><td>PT50M</td><td>4</td></tr>
<tr><td>PT3M</td><td>4</td></tr>
<tr><td>PT180M</td><td>4</td></tr>
<tr><td>PT7M</td><td>3</td></tr>
<tr><td>PT70M</td><td>3</td></tr>
<tr><td>PT2M</td><td>2</td></tr>
<tr><td>PT90M</td><td>2</td></tr>
</table>
only showing top 20 rows




### Implementing function to convert  'PT(?:\d+H)?(?:\d+M)?' pattern to minutes with integer type


```python
def cast_pt_time_to_minutes_integer(col):
    hours = coalesce(regexp_extract(col, r'(\d+)H', 1).cast('int'), lit(0))
    minutes = coalesce(regexp_extract(col, r'(\d+)M', 1).cast('int'), lit(0))
    return hours * 60 + minutes
```


```python
test_df = input_df

for columnName in ['cookTime', 'prepTime']:
    test_df = test_df.withColumn(columnName, cast_pt_time_to_minutes_integer(col(columnName)))
    
test_df
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>0</td><td>2013-04-01</td><td>Got leftover East...</td><td>http://static.the...</td><td>12 whole Hard Boi...</td><td>Easter Leftover S...</td><td>15</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2011-06-06</td><td>I finally have ba...</td><td>http://static.the...</td><td>3/4 cups Fresh Ba...</td><td>Pasta with Pesto ...</td><td>6</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2011-09-15</td><td>This was yummy. A...</td><td>http://static.the...</td><td>2 whole Pork Tend...</td><td>Herb Roasted Pork...</td><td>5</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>20</td><td>2012-04-23</td><td>I made this for a...</td><td>http://static.the...</td><td>1 pound Penne
4 w...</td><td>Chicken Florentin...</td><td>10</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>0</td><td>2011-06-13</td><td>Iced coffee is my...</td><td>http://static.the...</td><td>1 pound Ground Co...</td><td>Perfect Iced Coffee</td><td>480</td><td>24</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-05-31</td><td>When I was in Alb...</td><td>http://static.the...</td><td>1 whole Onion, Di...</td><td>Easy Green Chile ...</td><td>5</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>5</td><td>2013-03-25</td><td>Imagine the Easte...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Krispy Easter Eggs</td><td>20</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>25</td><td>2012-08-06</td><td>Who doesn&#x27;t love ...</td><td>http://static.the...</td><td>1 stick Butter
1 ...</td><td>Patty Melts</td><td>10</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>2</td><td>2012-08-10</td><td>Note from PW: On ...</td><td>http://static.the...</td><td> Doughnuts
1-1/8 ...</td><td>Yum. Doughnuts!</td><td>25</td><td>18</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2012-08-01</td><td>This is just a qu...</td><td>http://static.the...</td><td>1 pound Pasta (fe...</td><td>Buttery Lemon Par...</td><td>5</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>75</td><td>2012-08-13</td><td>There&#x27;s nothing s...</td><td>http://static.the...</td><td>1 whole Chicken, ...</td><td>Roast Chicken</td><td>10</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>60</td><td>2012-08-18</td><td>On this morning&#x27;s...</td><td>http://static.the...</td><td> FRENCH TOAST
 Bu...</td><td>Baked French Toast</td><td>15</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-08-15</td><td>I love slice-and-...</td><td>http://static.the...</td><td>2-1/2 cups All-pu...</td><td>Yummy Slice-and-B...</td><td>15</td><td>30</td><td>http://thepioneer...</td></tr>
<tr><td>20</td><td>2012-08-20</td><td>I love grilled ve...</td><td>http://static.the...</td><td>6 whole Zucchini ...</td><td>Yummy Grilled Zuc...</td><td>30</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-08-29</td><td>When we went to V...</td><td>http://static.the...</td><td>16 whole Graham C...</td><td>Chocolate Covered...</td><td>20</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>5</td><td>2012-08-22</td><td>I love compound b...</td><td>http://static.the...</td><td> HOTEL BUTTER
2 s...</td><td>T-Bone Steaks wit...</td><td>20</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>0</td><td>2012-08-31</td><td> I&#x27;ve got Mango M...</td><td>http://static.the...</td><td>2 whole Limes
2 T...</td><td>Mango Margaritas!</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>45</td><td>2012-01-04</td><td>I&#x27;m Pioneer Woman...</td><td>http://static.the...</td><td>5 cloves Garlic, ...</td><td>Tuscan Bean Soup ...</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>60</td><td>2011-12-30</td><td>I&#x27;m not a big bel...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Hoppin’ John</td><td>360</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2012-01-11</td><td>I would like to s...</td><td>http://static.the...</td><td>8 whole Everythin...</td><td>Turkey Bagel Burger</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
</table>
only showing top 20 rows




This function extracts hour and minutes from the columns and sum them with the result in minutes. When there is absence of a value, the extraction returns 0. But for the 101 Cookbook, a 0 is considered to be null if both columns (prepTime and cookTime) are 0.

This demonstrates that there are different rules for each website, so there is a need to split the data source by domain

### Splitting the data source based on website


```python
spark.sql("SELECT distinct regexp_extract(url, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1) FROM raw")
```




<table border='1'>
<tr><th>regexp_extract(url, ^(?:https?://)?(?:[^@/
]+@)?(?:www.)?([^:/?
]+), 1)</th></tr>
<tr><td>thepioneerwoman.com</td></tr>
<tr><td>101cookbooks.com</td></tr>
</table>




In this data source, as identified two websites: thepioneerwoman.com and 101cookbooks.com. Based on these values, I can split the dataset into two dataframes.


```python
input_df = input_df.withColumn('url_domain', regexp_extract(input_df.url, r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)', 1))

pioneer_woman_df = input_df.filter(input_df.url_domain.like('%thepioneerwoman.com')).drop('url_domain')
cook_books_df = input_df.filter(input_df.url_domain.like('%101cookbooks.com')).drop('url_domain')
```


```python
pioneer_woman_df
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>PT</td><td>2013-04-01</td><td>Got leftover East...</td><td>http://static.the...</td><td>12 whole Hard Boi...</td><td>Easter Leftover S...</td><td>PT15M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2011-06-06</td><td>I finally have ba...</td><td>http://static.the...</td><td>3/4 cups Fresh Ba...</td><td>Pasta with Pesto ...</td><td>PT6M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2011-09-15</td><td>This was yummy. A...</td><td>http://static.the...</td><td>2 whole Pork Tend...</td><td>Herb Roasted Pork...</td><td>PT5M</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>PT20M</td><td>2012-04-23</td><td>I made this for a...</td><td>http://static.the...</td><td>1 pound Penne
4 w...</td><td>Chicken Florentin...</td><td>PT10M</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>PT</td><td>2011-06-13</td><td>Iced coffee is my...</td><td>http://static.the...</td><td>1 pound Ground Co...</td><td>Perfect Iced Coffee</td><td>PT8H</td><td>24</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-05-31</td><td>When I was in Alb...</td><td>http://static.the...</td><td>1 whole Onion, Di...</td><td>Easy Green Chile ...</td><td>PT5M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT5M</td><td>2013-03-25</td><td>Imagine the Easte...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Krispy Easter Eggs</td><td>PT20M</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>PT25M</td><td>2012-08-06</td><td>Who doesn&#x27;t love ...</td><td>http://static.the...</td><td>1 stick Butter
1 ...</td><td>Patty Melts</td><td>PT10M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT2M</td><td>2012-08-10</td><td>Note from PW: On ...</td><td>http://static.the...</td><td> Doughnuts
1-1/8 ...</td><td>Yum. Doughnuts!</td><td>PT25M</td><td>18</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2012-08-01</td><td>This is just a qu...</td><td>http://static.the...</td><td>1 pound Pasta (fe...</td><td>Buttery Lemon Par...</td><td>PT5M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H15M</td><td>2012-08-13</td><td>There&#x27;s nothing s...</td><td>http://static.the...</td><td>1 whole Chicken, ...</td><td>Roast Chicken</td><td>PT10M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H</td><td>2012-08-18</td><td>On this morning&#x27;s...</td><td>http://static.the...</td><td> FRENCH TOAST
 Bu...</td><td>Baked French Toast</td><td>PT15M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-08-15</td><td>I love slice-and-...</td><td>http://static.the...</td><td>2-1/2 cups All-pu...</td><td>Yummy Slice-and-B...</td><td>PT15M</td><td>30</td><td>http://thepioneer...</td></tr>
<tr><td>PT20M</td><td>2012-08-20</td><td>I love grilled ve...</td><td>http://static.the...</td><td>6 whole Zucchini ...</td><td>Yummy Grilled Zuc...</td><td>PT30M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT10M</td><td>2012-08-29</td><td>When we went to V...</td><td>http://static.the...</td><td>16 whole Graham C...</td><td>Chocolate Covered...</td><td>PT20M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT5M</td><td>2012-08-22</td><td>I love compound b...</td><td>http://static.the...</td><td> HOTEL BUTTER
2 s...</td><td>T-Bone Steaks wit...</td><td>PT20M</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>PT</td><td>2012-08-31</td><td> I&#x27;ve got Mango M...</td><td>http://static.the...</td><td>2 whole Limes
2 T...</td><td>Mango Margaritas!</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT45M</td><td>2012-01-04</td><td>I&#x27;m Pioneer Woman...</td><td>http://static.the...</td><td>5 cloves Garlic, ...</td><td>Tuscan Bean Soup ...</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>PT1H</td><td>2011-12-30</td><td>I&#x27;m not a big bel...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Hoppin’ John</td><td>PT6H</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>PT15M</td><td>2012-01-11</td><td>I would like to s...</td><td>http://static.the...</td><td>8 whole Everythin...</td><td>Turkey Bagel Burger</td><td>PT10M</td><td>8</td><td>http://thepioneer...</td></tr>
</table>
only showing top 20 rows





```python
cook_books_df
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td></td><td>2009-07-06</td><td>From the Big Sur ...</td><td>http://www.101coo...</td><td>5 cups all-purpos...</td><td>Big Sur Bakery Hi...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT30M</td><td>2009-08-27</td><td>An old-fashioned ...</td><td>http://www.101coo...</td><td>1 cup plus 2 tabl...</td><td>Old-Fashioned Blu...</td><td>PT10M</td><td>Serves  8 - 10.</td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2009-10-25</td><td>An apple and carr...</td><td>http://www.101coo...</td><td>1/4 cup / 2 ounce...</td><td>Apple and Carrot ...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT15M</td><td>2009-11-15</td><td>Rustic orange-sce...</td><td>http://www.101coo...</td><td>3 cups whole whea...</td><td>Orange and Oat Sc...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT10M</td><td>2009-12-04</td><td>I made these for ...</td><td>http://www.101coo...</td><td>1/2 cup / 3.5 oz ...</td><td>Sparkling Ginger ...</td><td>PT30M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2009-12-09</td><td>This olive oil fl...</td><td>http://www.101coo...</td><td>4 1/2 cups / 1 lb...</td><td>Seeded Flatbread ...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT45M</td><td>2010-03-10</td><td>These jammy, fig-...</td><td>http://www.101coo...</td><td>Dry mix:
1 cup / ...</td><td>Figgy Buckwheat S...</td><td>PT100M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT25M</td><td>2010-03-30</td><td>These muffins are...</td><td>http://www.101coo...</td><td>butter to grease ...</td><td>Lucia Muffins</td><td>PT20M</td><td>Makes 10 - 12 muf...</td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2010-04-10</td><td>Dense, gooey choc...</td><td>http://www.101coo...</td><td>butter for greasi...</td><td>Chocolate Cherry ...</td><td>PT900M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT60M</td><td>2010-04-24</td><td>A rustic, minimal...</td><td>http://www.101coo...</td><td>butter to grease ...</td><td>Quinoa Skillet Bread</td><td>PT10M</td><td>Makes one 10 1/2 ...</td><td>http://www.101coo...</td></tr>
<tr><td>PT40M</td><td>2010-05-04</td><td>A simple spring c...</td><td>http://www.101coo...</td><td>butter for greasi...</td><td>Strawberry Rhubar...</td><td>PT20M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT15M</td><td>2010-05-14</td><td>Cookies made from...</td><td>http://www.101coo...</td><td>3/4 cup / 3.5 oz ...</td><td>Quinoa Cloud Cookies</td><td>PT60M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT45M</td><td>2010-05-23</td><td>A cake I made for...</td><td>http://www.101coo...</td><td>Olive oil for the...</td><td>Rosemary Olive Oi...</td><td>PT15M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td>PT50M</td><td>2010-06-27</td><td>Certainly not the...</td><td>http://www.101coo...</td><td>zest of one lemon...</td><td>Chocolate Loaf Cake</td><td>PT15M</td><td>Makes 8 - 10 slices.</td><td>http://www.101coo...</td></tr>
<tr><td>PT40M</td><td>2010-05-31</td><td>Inspired by Hugh ...</td><td>http://www.101coo...</td><td>2 1/2 tablespoons...</td><td>Six-seed Soda Bre...</td><td>PT10M</td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2003-05-27</td><td>101 Cookbooks: Pe...</td><td>http://www.101coo...</td><td>Salt
One 35-ounce...</td><td></td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2003-06-29</td><td>101 Cookbooks: Sc...</td><td>http://www.101coo...</td><td>6 ounces Scharffe...</td><td>Scharffen Berger ...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-09-06</td><td>101 Cookbooks: Ch...</td><td>http://www.101coo...</td><td>1 lb. Jacob&#x27;s Cat...</td><td>Chocolate Calypso...</td><td></td><td></td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-10-20</td><td>101 Cookbooks: Ap...</td><td>http://www.101coo...</td><td>3 tablespoons all...</td><td>Apple Pie Recipe</td><td></td><td>Serves 6 to 8
Act...</td><td>http://www.101coo...</td></tr>
<tr><td></td><td>2004-11-10</td><td>Nancy Silverton&#x27;s...</td><td>http://www.101coo...</td><td>2 1/2 cups plus 2...</td><td>Graham Cracker Re...</td><td></td><td></td><td>http://www.101coo...</td></tr>
</table>
only showing top 20 rows




### Implementing domain rules for Pioneer Woman

It was not identified any special rule for this half of the data source, but it is a good practice to include the *cast_pt_time_to_minutes_integer* function here, as if it was called for the whole data source, a new website could not follow the same structure.


```python
def apply_pioneer_woman_rules(df):
    for columnName in ['cookTime', 'prepTime']:
        df = df.withColumn(columnName, cast_pt_time_to_minutes_integer(col(columnName)))
    return df
```

### Implementing domain rules for 101 Cookbook

- If cookTime and prepTime are both 0, then they should be null


```python
def transform_cook_prepare_time(df):
    for columnName in ['cookTime', 'prepTime']:
        df = df.withColumn(columnName, when(col(columnName) != '', cast_pt_time_to_minutes_integer(col(columnName))))
    return df
```

- If name is empty, then it should be searched on url, description and image columns


```python
def extract_recipe_name(df):
    def extract_recipe_name_from_url(col):
        url_last_param = reverse(regexp_extract(reverse(col), '(?i)/?(.+?)/', 1))
        param_without_extension_and_numbers = regexp_replace(url_last_param, r'\d+|(?:\..+)', '')
        recipe_name = initcap(translate(param_without_extension_and_numbers, '-', ' '))
        return when(param_without_extension_and_numbers != '', recipe_name)
    
    def extract_recipe_name_from_description(col):
        description_extract = regexp_extract(df.description, r'101\s+Cookbooks:\s+((?:(?:\w+)|(?:\s+))+)', 1)
        return when(description_extract != '', description_extract)
        
    name_from_description = extract_recipe_name_from_description(df.description)
    name_from_url = extract_recipe_name_from_url(df.url)
    name_from_image = extract_recipe_name_from_url(df.image)

    return when(df.name != '', df.name).otherwise(coalesce(name_from_description,name_from_url,name_from_image))
```

- If recipeYield has more than one line, only the first should me considered


```python
def extract_recipe_yield(col):
    return split(col, '\n').getItem(0)
```


```python
def apply_cookbooks_rules(df):
    df = transform_cook_prepare_time(df)
    df = df.withColumn('name', extract_recipe_name(df)).withColumn('recipeYield', extract_recipe_yield(df.recipeYield))
    return df
```

### Consolidate partitions results


```python
domain_rules = {
    'thepioneerwoman.com': apply_pioneer_woman_rules,
    '101cookbooks.com': apply_cookbooks_rules
}

input_df = input_df.withColumn('datePublished', input_df.datePublished.cast('date'))

input_df_partitions = [
    rule(input_df.filter(input_df.url_domain.like(f'%{url}')).drop('url_domain'))
    for url, rule in domain_rules.items()
]

result_df = reduce(DataFrame.unionAll, input_df_partitions)
```


```python
result_df
```




<table border='1'>
<tr><th>cookTime</th><th>datePublished</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>0</td><td>2013-04-01</td><td>Got leftover East...</td><td>http://static.the...</td><td>12 whole Hard Boi...</td><td>Easter Leftover S...</td><td>15</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2011-06-06</td><td>I finally have ba...</td><td>http://static.the...</td><td>3/4 cups Fresh Ba...</td><td>Pasta with Pesto ...</td><td>6</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2011-09-15</td><td>This was yummy. A...</td><td>http://static.the...</td><td>2 whole Pork Tend...</td><td>Herb Roasted Pork...</td><td>5</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>20</td><td>2012-04-23</td><td>I made this for a...</td><td>http://static.the...</td><td>1 pound Penne
4 w...</td><td>Chicken Florentin...</td><td>10</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>0</td><td>2011-06-13</td><td>Iced coffee is my...</td><td>http://static.the...</td><td>1 pound Ground Co...</td><td>Perfect Iced Coffee</td><td>480</td><td>24</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-05-31</td><td>When I was in Alb...</td><td>http://static.the...</td><td>1 whole Onion, Di...</td><td>Easy Green Chile ...</td><td>5</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>5</td><td>2013-03-25</td><td>Imagine the Easte...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Krispy Easter Eggs</td><td>20</td><td>12</td><td>http://thepioneer...</td></tr>
<tr><td>25</td><td>2012-08-06</td><td>Who doesn&#x27;t love ...</td><td>http://static.the...</td><td>1 stick Butter
1 ...</td><td>Patty Melts</td><td>10</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>2</td><td>2012-08-10</td><td>Note from PW: On ...</td><td>http://static.the...</td><td> Doughnuts
1-1/8 ...</td><td>Yum. Doughnuts!</td><td>25</td><td>18</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2012-08-01</td><td>This is just a qu...</td><td>http://static.the...</td><td>1 pound Pasta (fe...</td><td>Buttery Lemon Par...</td><td>5</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>75</td><td>2012-08-13</td><td>There&#x27;s nothing s...</td><td>http://static.the...</td><td>1 whole Chicken, ...</td><td>Roast Chicken</td><td>10</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>60</td><td>2012-08-18</td><td>On this morning&#x27;s...</td><td>http://static.the...</td><td> FRENCH TOAST
 Bu...</td><td>Baked French Toast</td><td>15</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-08-15</td><td>I love slice-and-...</td><td>http://static.the...</td><td>2-1/2 cups All-pu...</td><td>Yummy Slice-and-B...</td><td>15</td><td>30</td><td>http://thepioneer...</td></tr>
<tr><td>20</td><td>2012-08-20</td><td>I love grilled ve...</td><td>http://static.the...</td><td>6 whole Zucchini ...</td><td>Yummy Grilled Zuc...</td><td>30</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>10</td><td>2012-08-29</td><td>When we went to V...</td><td>http://static.the...</td><td>16 whole Graham C...</td><td>Chocolate Covered...</td><td>20</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>5</td><td>2012-08-22</td><td>I love compound b...</td><td>http://static.the...</td><td> HOTEL BUTTER
2 s...</td><td>T-Bone Steaks wit...</td><td>20</td><td>4</td><td>http://thepioneer...</td></tr>
<tr><td>0</td><td>2012-08-31</td><td> I&#x27;ve got Mango M...</td><td>http://static.the...</td><td>2 whole Limes
2 T...</td><td>Mango Margaritas!</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>45</td><td>2012-01-04</td><td>I&#x27;m Pioneer Woman...</td><td>http://static.the...</td><td>5 cloves Garlic, ...</td><td>Tuscan Bean Soup ...</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
<tr><td>60</td><td>2011-12-30</td><td>I&#x27;m not a big bel...</td><td>http://static.the...</td><td>4 Tablespoons But...</td><td>Hoppin’ John</td><td>360</td><td>10</td><td>http://thepioneer...</td></tr>
<tr><td>15</td><td>2012-01-11</td><td>I would like to s...</td><td>http://static.the...</td><td>8 whole Everythin...</td><td>Turkey Bagel Burger</td><td>10</td><td>8</td><td>http://thepioneer...</td></tr>
</table>
only showing top 20 rows





```python
result_df.printSchema()
```

    root
     |-- cookTime: integer (nullable = true)
     |-- datePublished: date (nullable = true)
     |-- description: string (nullable = true)
     |-- image: string (nullable = true)
     |-- ingredients: string (nullable = true)
     |-- name: string (nullable = true)
     |-- prepTime: integer (nullable = true)
     |-- recipeYield: string (nullable = true)
     |-- url: string (nullable = true)
    



```python
result_df.describe()
```




<table border='1'>
<tr><th>summary</th><th>cookTime</th><th>description</th><th>image</th><th>ingredients</th><th>name</th><th>prepTime</th><th>recipeYield</th><th>url</th></tr>
<tr><td>count</td><td>716</td><td>1042</td><td>1042</td><td>1042</td><td>1042</td><td>739</td><td>1042</td><td>1042</td></tr>
<tr><td>mean</td><td>32.65782122905028</td><td>null</td><td>null</td><td>null</td><td>null</td><td>40.53585926928282</td><td>9.738404452690167</td><td>null</td></tr>
<tr><td>stddev</td><td>48.144380267381514</td><td>null</td><td>null</td><td>null</td><td>null</td><td>119.58444193548257</td><td>6.529901914334942</td><td>null</td></tr>
<tr><td>min</td><td>0</td><td>      IMPORTANT: ...</td><td>http://static.the...</td><td> (Quantities Depe...</td><td>A Blast From the ...</td><td>0</td><td></td><td>http://thepioneer...</td></tr>
<tr><td>max</td><td>540</td><td>   We just had a ...</td><td>http://www.101coo...</td><td>~1 1/2 cups (7 ou...</td><td>Zucchini Ricotta ...</td><td>1440</td><td>Serves six.</td><td>http://www.101coo...</td></tr>
</table>




### Saving result as Parquet


```python
execution_date = date.today().strftime('%Y-%m-%d')
result_df.coalesce(20).write.parquet(f'/tmp/result/execution_date={execution_date}/')
# result_df.toPandas().to_excel('/tmp/result.xlsx')
```

This file format was chosen because of the following:
- As a compression-optimized, metadata-oriented format, it will have an advantage over plaintext formats, like JSON, CSV.
- As a columnar format, it will have an advantage on ad-hoc queries on columns compared to Avro
- The README from this challenge said that it was used by Kafka extraction jobs, so there is a familiarity/preference of HelloFresh, compared to ORC

The **coalesce** method was used to reduce the possible amount of small files persisted by the write method. This method was used instead of **repartition** to prevent shuffling of data between Spark nodes, at the cost of unbalanced file sizes.

I did not partitioned the dataframe by **datePublished**, because this JSON is a dump from the table, containing historical data. Also, I forced a partition value directly on the write path, was it would be wasteful to create a new column with the execution_date and tell Spark to partition the output by it.

If the process that generates this JSON would just provide the updated/deleted/created information, instead of the entire table, I would consider using a file format that enables CRUD operations (like Apache Hudi, Apache Iceberg and Databricks DeltaLake), and partition by **datePublished** while configuring the history of the file to be kept indefinitely, at the expense of some performance.

Also, if the URL domain could be used as a partition, I would skip entirely the UNION step and persist which partition directly on the filesystem, like the following.


```python
# execution_date = date.today().strftime('%Y-%m-%d')

# for url, rule in domain_rules.items():
#     result_df = rule(input_df.filter(input_df.url_domain.like(f'%{url}')).drop('url_domain'))
#     result_df.coalesce(20).write.parquet(f'/tmp/result/execution_date={execution_date}/url_domain={url}')
```

# Task 2

### Filtering only recipes that has 'beef' as ingredient

At first, I was only going to consider literal **beef** (ignoring case) on ingredients, but after validating that some recipes did have 'beef' on its name or description, I decided that the ingredient of these recipes should be analyzed.


```python
result_df.createOrReplaceTempView('result')
```


```python
spark.sql("""
    SELECT count(1) 
    FROM result 
    WHERE lower(ingredients) LIKE '%beef%'
""")
```




<table border='1'>
<tr><th>count(1)</th></tr>
<tr><td>47</td></tr>
</table>





```python
spark.sql("""
    SELECT url
    FROM result 
    WHERE lower(ingredients) NOT LIKE '%beef%' 
    AND (lower(description) LIKE '%beef%' OR lower(name) LIKE '%beef%') 
""").show(10, False)
```

    +-------------------------------------------------------------------------+
    |url                                                                      |
    +-------------------------------------------------------------------------+
    |http://thepioneerwoman.com/cooking/2011/06/caprese-salad/                |
    |http://thepioneerwoman.com/cooking/2010/10/beef-with-snow-peas/          |
    |http://thepioneerwoman.com/cooking/2010/08/beer-braised-beef-with-onions/|
    |http://thepioneerwoman.com/cooking/2011/03/beef-with-peppers/            |
    |http://thepioneerwoman.com/cooking/2011/05/beef-noodle-salad-bowls/      |
    |http://thepioneerwoman.com/cooking/2011/02/beef-fajita-nachos/           |
    |http://thepioneerwoman.com/cooking/2010/10/chicken-cacciatore/           |
    |http://thepioneerwoman.com/cooking/2007/06/olive_cheese_br/              |
    |http://www.101cookbooks.com/archives/cracker-lasagna-recipe.html         |
    +-------------------------------------------------------------------------+
    


Then I manually visited these 9 URL's to see the recipe photo and description. I turned out that some occurrences were just mentions and did not mean that the recipe had any beef. But for the four occurrences that really had a beef on the photo, the word 'steak' was on the ingredients list.


```python
from pyspark.ml.feature import StopWordsRemover

remover = StopWordsRemover(inputCol='ingredients', outputCol='final')

df = spark.sql(r"""
    SELECT split(ingredients_element, ' ') AS ingredients
    FROM (
        SELECT explode(split(regexp_replace(lower(ingredients), '[^a-z ]', ''), '\n')) AS ingredients_element
        FROM result
        WHERE lower(ingredients) NOT LIKE '%beef%' 
        AND (lower(description) LIKE '%beef%' OR lower(name) LIKE '%beef%') 
    )
""")

remover.transform(df).where('size(ingredients) > size(final)')\
       .select(explode(array_distinct(col('final'))).alias('ingredients'))\
       .groupby(col('ingredients')).count().orderBy(col('count').desc())
```




<table border='1'>
<tr><th>ingredients</th><th>count</th></tr>
<tr><td></td><td>8</td></tr>
<tr><td>oil</td><td>8</td></tr>
<tr><td>whole</td><td>7</td></tr>
<tr><td>olive</td><td>7</td></tr>
<tr><td>sliced</td><td>7</td></tr>
<tr><td>pepper</td><td>6</td></tr>
<tr><td>cup</td><td>6</td></tr>
<tr><td>pound</td><td>5</td></tr>
<tr><td>weight</td><td>5</td></tr>
<tr><td>cloves</td><td>5</td></tr>
<tr><td>garlic</td><td>5</td></tr>
<tr><td>tablespoons</td><td>5</td></tr>
<tr><td>fresh</td><td>5</td></tr>
<tr><td>onion</td><td>5</td></tr>
<tr><td>ounces</td><td>5</td></tr>
<tr><td>teaspoon</td><td>5</td></tr>
<tr><td>salt</td><td>5</td></tr>
<tr><td>sauce</td><td>4</td></tr>
<tr><td>steak</td><td>4</td></tr>
<tr><td>brown</td><td>4</td></tr>
</table>
only showing top 20 rows




In the end, I preferred to not include any other word on my search.

### Averaging the total cook time based on recipe's difficulty


```python
spark.sql("""
    SELECT CASE WHEN total_cook_time < 30 THEN 'easy' 
                WHEN total_cook_time < 60 THEN 'medium'
                ELSE 'hard ' END AS difficulty, AVG(total_cook_time) AS avg_total_cooking_time 
    FROM (
        SELECT IFNULL(cookTime, 0) + IFNULL(prepTime, 0) AS total_cook_time
        FROM result
        WHERE IFNULL(cookTime, prepTime) IS NOT NULL
    )
    GROUP BY 1
""")
```




<table border='1'>
<tr><th>difficulty</th><th>avg_total_cooking_time</th></tr>
<tr><td>hard </td><td>149.48863636363637</td></tr>
<tr><td>medium</td><td>39.37065637065637</td></tr>
<tr><td>easy</td><td>17.02314814814815</td></tr>
</table>





```python
filtered_df = result_df.filter(coalesce(result_df.cookTime, result_df.prepTime).isNotNull())
sum_df = filtered_df.select((coalesce(filtered_df.cookTime, lit(0)) + coalesce(filtered_df.prepTime, lit(0))).alias('total_cook_time'))
classified_df = sum_df.select(when(sum_df.total_cook_time < 30, 'easy').when(sum_df.total_cook_time < 60, 'medium').otherwise('hard').alias('difficulty'), sum_df.total_cook_time)
result_df = classified_df.groupby('difficulty').agg(avg(sum_df.total_cook_time).alias('avg_total_cooking_time'))
result_df
```




<table border='1'>
<tr><th>difficulty</th><th>avg_total_cooking_time</th></tr>
<tr><td>medium</td><td>39.37065637065637</td></tr>
<tr><td>hard</td><td>149.48863636363637</td></tr>
<tr><td>easy</td><td>17.02314814814815</td></tr>
</table>





```python
result_df.repartition(1).write.csv('/tmp/output', header=True)
```
