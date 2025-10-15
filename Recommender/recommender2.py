# exec(open("recommender.py").read())
# 
# The script successfully built a collaborative filtering recommendation model using the Alternating Least Squares (ALS) algorithm. It tuned a key parameter, tested the model's accuracy, and then generated personalized movie recommendations for a new user.
# 
# Code Breakdown
# 
# 1. Data Loading and Preparation: It starts by loading the ratings.csv file into a Resilient Distributed Dataset (RDD). Each line is parsed into a tuple of (userID, movieID, rating). This dataset is then split into three parts: a training set (60%) to build the model, a validation set (20%) to tune its parameters, and a test set (20%) to evaluate the final model's performance.
# 
# 2. Hyperparameter Tuning (Finding best_k): The model's performance is sensitive to its configuration. The script tests several values for the rank (k = 4, 8, 12), which is the number of latent factors the model uses to characterize users and movies.
# 
# For each rank, it trains an ALS model on the training_RDD.
# 
# It then makes predictions for the users and movies in the validation_RDD.
# 
# It calculates the Root Mean Square Error (RMSE) to measure the difference between the model's predicted ratings and the actual ratings. RMSE is a standard way to measure the error of a prediction model; a lower RMSE means a more accurate model. 
# The script found that a rank of k=8 produced the lowest RMSE (0.941) on the validation data.
# 
# 3. Final Model Evaluation: Using the best rank (k=8), a final model is trained on the training data and then evaluated against the completely unseen test_RDD. This gives a more objective measure of its performance, which was an RMSE of 0.953. For a 5-star rating system, an RMSE under 1.0 is generally considered a good result.
# 
# 4. Generating Recommendations: A new user (ID 0) is created with 10 sample movie ratings. The model is retrained on the entire dataset plus this new user's ratings.
# The script then identifies all movies the new user has not rated. Finally, it uses the updated model to predict a rating for each of those unrated movies and displays the top 10 highest-rated predictions.

# --------------

# ---

# ### 1. Creating and Adding the New User

# This section simulates a new user joining the platform and providing some initial ratings.

# * `new_user_ID = 0`: A unique ID (`0`) is assigned to the new user. The comment correctly notes that you'd first want to ensure this ID isn't already taken.
# * `new_user = [...]`: A standard Python list is created to hold the new user's ratings. Each item is a tuple with the format `(userID, movieID, rating)`. For example, `(0, 100, 4)` means user `0` rated movie `100` a `4`.
# * `new_user_RDD = sc.parallelize(new_user)`: Spark can't work with a regular Python list directly in distributed operations. The `parallelize` command converts this list into a Spark **Resilient Distributed Dataset (RDD)**, which is the fundamental data structure in Spark.
# * `updated_ratings_RDD = ratings_RDD.union(new_user_RDD)`: The `union` operation merges the original, large RDD of all user ratings (`ratings_RDD`) with the new user's ratings RDD (`new_user_RDD`). The result is a complete dataset that now includes the new user.

# ---

# ### 2. Retraining the Model

# For the system to understand the new user's preferences, the model must be updated.

# * `updated_model = ALS.train(...)`: This is a crucial and computationally intensive step. The `ALS.train` function is called again, but this time it's trained on the `updated_ratings_RDD`. By including the new user's 10 ratings in the training data, the model learns a **latent factor vector** for this user, which mathematically represents their tastes.

# ---

# ### 3. Finding Movies to Recommend

# You don't want to recommend movies the user has already seen. This section builds a list of movies that are candidates for recommendation.

# * `movies_RDD = ...`: The `movies.csv` file is loaded and parsed into an RDD of `(movieID, title)` pairs. This represents the entire catalog of available movies.
# * `new_user_rated_movie_ids = map(...)`: This line creates a simple list of just the **movie IDs** that the new user has already rated (e.g., `[100, 237, 44, ... ]`).
# * `new_user_unrated_movies_RDD = movies_RDD.filter(...)`: This is the key filtering step.
#     1.  It starts with the RDD of *all* movies.
#     2.  The `.filter(lambda x: x[0] not in new_user_rated_movie_ids)` part removes any movie whose ID is in the "already rated" list.
#     3.  The `.map(lambda x: (new_user_ID, x[0]))` part then transforms the remaining movies into the `(userID, movieID)` format that the prediction model needs.

# The result is a large RDD containing a pair for every single movie the new user has **not** yet rated.

# ---

# ### 4. Making the Predictions

# This is the final step where the magic happens. ✨

# * `new_user_recommendations_RDD = updated_model.predictAll(...)`: The retrained model (`updated_model`) is used to predict a rating for every single `(user, movie)` pair in the `new_user_unrated_movies_RDD`.

# The output, `new_user_recommendations_RDD`, is an RDD containing the new user's ID, the movie's ID, and the model's **predicted rating** for that movie. The next steps in your script (which are not shown here but are in the full file) would be to sort this RDD by the predicted rating in descending order and take the top 10 results.

# -----------------------------

#Use large or small datasets
ratings_raw_RDD = sc.textFile('ratings.csv')
# ratings_raw_RDD = sc.textFile('ratings-large.csv')

#Parse lines
ratings_RDD = ratings_raw_RDD.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2])))

#Split into training, validation and test sets
training_RDD, validation_RDD, test_RDD = ratings_RDD.randomSplit([3, 1, 1], 0)

#Create prediction sets without ratings
predict_validation_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
predict_test_RDD = test_RDD.map(lambda x: (x[0], x[1]))

from pyspark.mllib.recommendation import ALS
import math

seed = 5
iterations = 10
regularization = 0.1
trial_ranks = [4, 8, 12]
lowest_error = float('inf')

for k in trial_ranks:
    model = ALS.train(training_RDD, k, seed=seed, iterations=iterations, lambda_=regularization)
    #Coercing ((u,p),r) tuple format to accomodate join
    predictions_RDD = model.predictAll(predict_validation_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    ratings_and_preds_RDD = validation_RDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions_RDD)
    error = math.sqrt(ratings_and_preds_RDD.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    print ('For k=',k,'the RMSE is', error)
    if error < lowest_error:
        best_k = k
        lowest_error = error

print('The best rank is size', best_k)

import matplotlib.pyplot as plt
from pyspark.mllib.recommendation import ALS
import math

# Assumes training_RDD, validation_RDD, predict_validation_RDD, and best_k are pre-loaded
# best_k was determined to be 8 in the previous step.

iterations_range = range(1, 15)
error_values = []
best_k = 8
seed = 5
regularization = 0.1

print("Analyzing model convergence...")

# Loop through different numbers of iterations
for i in iterations_range:
    # Train the model with i iterations
    model = ALS.train(training_RDD, best_k, seed=seed, iterations=i, lambda_=regularization)
    
    # Make predictions on the validation data
    predictions_RDD = model.predictAll(predict_validation_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    ratings_and_preds_RDD = validation_RDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions_RDD)
    
    # Calculate the RMSE
    rmse = math.sqrt(ratings_and_preds_RDD.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    error_values.append(rmse)
    print(f'For {i} iterations, RMSE = {rmse}')

print("Analysis complete.")

# --- Plotting the results ---
plt.figure(figsize=(10, 6))
plt.plot(iterations_range, error_values, marker='o', linestyle='--')
plt.title('ALS Model Convergence: RMSE vs. Iterations')
plt.xlabel('Number of Iterations')
plt.ylabel('Root Mean Square Error (RMSE)')
plt.grid(True)
plt.xticks(iterations_range)
plt.tight_layout()

# Save the plot to a file to view it
plt.savefig('convergence_plot.png')

print("Convergence plot saved as convergence_plot.png")


# ---
 
# 1) We noticed that our top ranked movies have ratings higher than 5. This makes perfect sense as there is no ceiling implied in our algorithm and one can imagine that certain combinations of factors would combine to create “better than anything you’ve seen yet” ratings. Maybe you have a friend that really likes Anime. Many of her ratings for Anime are 5. And she really likes Scarlett Johansson and gives her movies lots of 5s. Wouldn’t it be fair to consider her rating for Ghost in the Shell to be a 7/5? Nevertheless, we may have to constrain our ratings to a 1-5 range. Can you normalize the output from our recommender such that our new users only sees ratings in that range?

# 2) We haven’t really investigated our convergence rate. We specify 10 iterations, but is that reasonable? Graph your error against iterations and see if that is a good number.

# 3) I mentioned that our larger dataset does benefit from a rank of 12 instead of 4 (as one might expect). The larger datasets (ratings-large.csv and movies-large.csv) are available to you in ~training/LargeMovies. Prove that the error is less with a larger rank. How does this dataset benefit from more iterations? Is it more effective to spend the computation cycles on more iterations or larger ranks?

# 4) We could have used the very similar pyspark.ml.recommendation API, which uses dataframes. It requires a little more type checking, so we used the classic RDD API pyspark.mllib.recommendation instead - for conciseness. Try porting this example to that API. Is this a better way to work?

# 5) (Advanced) When we added our new user, we also retrained with the full ratings RDD. We no longer excluded the test and validation data. This seems reasonable, but are we still sure that our hyperparameters remain valid? And this will remain an issue as we continue to add new users and movies. You should investigate the sensitivity of our various hyperparameters to Ratings size, and consider how to deal with this problem.