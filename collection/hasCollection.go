package collection

// HasCollection проверяет существование коллекции
func (cm *CollectionManager) HasCollection(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.collections[name]
	return exists
}
