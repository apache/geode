import org.gradle.api.artifacts.ModuleDependency

  def boolean hasExtension(ModuleDependency dependency,String key){
    return dependency.ext.has(key)
  }
