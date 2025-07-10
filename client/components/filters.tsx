"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Filter, X, MapPin, Building } from "lucide-react"

interface FiltersProps {
  onFilterChange: (filters: { state: string; city: string }) => void
  currentFilters: { state: string; city: string }
}

export function Filters({ onFilterChange, currentFilters }: FiltersProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [tempFilters, setTempFilters] = useState(currentFilters)

  const handleApplyFilters = () => {
    onFilterChange(tempFilters)
    setIsOpen(false)
  }

  const handleClearFilters = () => {
    const emptyFilters = { state: "", city: "" }
    setTempFilters(emptyFilters)
    onFilterChange(emptyFilters)
    setIsOpen(false)
  }

  const hasActiveFilters = currentFilters.state || currentFilters.city

  return (
    <div className="relative">
      <Button
        variant="outline"
        onClick={() => setIsOpen(!isOpen)}
        className={`bg-white/60 backdrop-blur-sm border-white/20 hover:bg-white/80 ${
          hasActiveFilters ? "border-blue-300 bg-blue-50/60" : ""
        }`}
      >
        <Filter className="w-4 h-4 mr-2" />
        Фильтры
        {hasActiveFilters && (
          <Badge variant="secondary" className="ml-2 bg-blue-100 text-blue-800">
            {(currentFilters.state ? 1 : 0) + (currentFilters.city ? 1 : 0)}
          </Badge>
        )}
      </Button>

      {isOpen && (
        <Card className="absolute top-12 right-0 w-80 bg-white/90 backdrop-blur-sm border-white/20 shadow-xl z-20">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center justify-between">
              Фильтры поиска
              <Button variant="ghost" size="sm" onClick={() => setIsOpen(false)}>
                <X className="w-4 h-4" />
              </Button>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700 flex items-center">
                <MapPin className="w-4 h-4 mr-1" />
                Штат
              </label>
              <Input
                placeholder="Например: CA, NY, FL"
                value={tempFilters.state}
                onChange={(e) => setTempFilters({ ...tempFilters, state: e.target.value })}
                className="bg-white/50 border-white/20"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700 flex items-center">
                <Building className="w-4 h-4 mr-1" />
                Город
              </label>
              <Input
                placeholder="Например: New York, Los Angeles"
                value={tempFilters.city}
                onChange={(e) => setTempFilters({ ...tempFilters, city: e.target.value })}
                className="bg-white/50 border-white/20"
              />
            </div>

            <div className="flex space-x-2 pt-2">
              <Button onClick={handleApplyFilters} className="flex-1 bg-gradient-to-r from-blue-600 to-indigo-600">
                Применить
              </Button>
              <Button variant="outline" onClick={handleClearFilters} className="flex-1 bg-transparent">
                Очистить
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
