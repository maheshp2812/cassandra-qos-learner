from sklearn import tree
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_val_predict
import matplotlib.pyplot as plt
import math
import sys

# Get training input X and y from file f
def extractData(lines):
	X = [list(map(float, x.split(",")[:-2])) for x in lines]
	y = [float(line.split(",")[-1]) for line in lines]
	return [X, y]

f = open("data.csv", "r")
lines = f.readlines()[1:]

split = int(len(lines) * 0.7)
trainingData = lines[1:split]
testData = lines[split:]

[X, y] = extractData(lines)
# [X1, y1] = extractData(trainingData)
# [X2, y2] = extractData(testData)

clf = tree.DecisionTreeRegressor(max_depth=int(sys.argv[1]))

# Fit the regression model with training data
clf = clf.fit(X, y)
# Export regression model
tree.export_graphviz(clf, out_file="tree.dot")

def testAccuracy(outputs,predictions):
	rse = 0
	for i in range(len(predictions)):
		rse += math.sqrt(abs(outputs[i]**2 - predictions[i]**2))
	return rse/len(predictions)

predictions = clf.predict(X2)
# print(testAccuracy(y2, predictions))
# print(list(cross_val_predict(clf, X, y)))
scores = cross_val_score(clf, X, y, cv=10, scoring='r2')
print(scores)
print('Accuracy:', scores.mean()*100, '%')

