import { Card, CardContent, CardHeader } from "@/components/ui/card"

export function LoadingSkeleton() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {Array.from({ length: 6 }).map((_, index) => (
        <Card key={index} className="bg-white/60 backdrop-blur-sm border-white/20 shadow-lg animate-pulse">
          <CardHeader className="pb-3">
            <div className="flex items-start justify-between">
              <div className="space-y-2 flex-1">
                <div className="h-6 bg-gray-200 rounded-md w-3/4"></div>
                <div className="h-4 bg-gray-200 rounded-md w-1/4"></div>
              </div>
              <div className="w-3 h-3 bg-gray-200 rounded-full"></div>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 bg-gray-200 rounded"></div>
              <div className="h-4 bg-gray-200 rounded-md flex-1"></div>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 bg-gray-200 rounded"></div>
              <div className="h-4 bg-gray-200 rounded-md flex-1"></div>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 bg-gray-200 rounded"></div>
              <div className="h-4 bg-gray-200 rounded-md flex-1"></div>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 bg-gray-200 rounded"></div>
              <div className="h-4 bg-gray-200 rounded-md flex-1"></div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
