import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
from sklearn import tree
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_val_predict
import math
import sys

# System arguments
if len(sys.argv[1:]) == 4:
	filename = sys.argv[1]
	outputfile = sys.argv[2]
	mode = int(sys.argv[3])
	depth = int(sys.argv[4])
else:
	print("Usage: python3 RegressionTreeBuilder.py <filename> <outputfile> <read/write mode> <depth>")
	sys.exit(0)

# Get training input X and y from file f
def extractData(lines):
	X = [list(map(float, x.split(",")[:-2])) for x in lines]
	y = [float(line.split(",")[-mode]) for line in lines]
	return [X, y]

f = open(filename, "r")
lines = f.readlines()[1:]

[X, y] = extractData(lines)

# depth_range = range(1, 20)
scores_range = []
# for i in depth_range:
clf = tree.DecisionTreeRegressor(max_depth = depth)
clf = clf.fit(X, y)
pop = sum(y) / len(y)
scores = cross_val_score(clf, X, y, cv = 10, scoring = 'neg_mean_squared_error')
scores = -scores
scores_range.append(scores.mean())

print('Ratio: ', math.sqrt(scores.mean())/pop)
tree.export_graphviz(clf,out_file = outputfile)
# plt.plot(depth_range, scores_range)
# plt.savefig(outputfile)
