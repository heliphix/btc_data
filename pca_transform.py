import numpy as np

class PCA:
	def __init__(self, n_components):
		self.n_components = n_components
		self.components = None
		self.mean = None
		
	def fit(self, X):
		#mean centering
		self.mean = np.mean(X, axis=0)
		X = X - self.mean
		
		#covariance (needs samples as columns)
		cov = np.cov(X.T)
		
		
		# eigenvalues, eigenvectors
		eigenvalues, eigenvectors = np.linalg.eig(cov)
		
		# -> eigenvector v = [:,i]
		# transpore for easier calculations
		eigenvectors = eigenvectors.T
		
		# sort eigenvectors
		idxs = np.argsort(eigenvalues)[::-1]
		eigenvalues = eigenvalues[idxs]
		eigenvectors = eigenvectors[idxs]
		
		#store first n eigenvectors
		self.components = eigenvectors[0 : self.n_components]
		
	def transform(self, X)
		#project data
		X= X - self.mean
		return np.dot(X, self.components.T)
		
# project the data onto the 2 primary principal components

pca = PCA(2)
pca.fit(X)
X_projected = pca.transform(X)		